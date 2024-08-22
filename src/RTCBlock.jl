module RTCBlock
using Hsm
using Aeron
using ThreadPinning # Specify these through the JULIA_LIKWID_PIN environment variable.
using SpidersMessageEncoding

"""
    RTCBlock.statevariables(sm::MyStateMachine) -> Named Tuple

Define a method of this function for your state machine type
that returns a named tuple of variables that describe the outwardly
visible state of the block.
When a new subscriber joins, they can request the current state
after which these values are sent to the status channel.
"""
function statevariables end

"""
    serve(sm::Hsm.AbstractStateMachine)

Set up subscription and publication streams, and then act as a server
handing off data to a provided state machine.


Control the streams used via environment variables:
- PUB_STATUS_URI
- PUB_STATUS_STREAM
- SUB_CONTROL_URI
- SUB_CONTROL_STREAM
- SUB_DATA_URI_i
- SUB_DATA_STEAM_i

where i starts at 1. Zero or more data streams are supported.

Your state machine must have an :Error state defined. If an exception
occurs, the state machine will be transitioned to that state. 

After dispatching events, `serve` handles reporting the status
of the state machine to any listeners if the current state has changed.

Similarily, if while processing events an 
"""
serve(
    sm::Hsm.AbstractStateMachine,
    event_queue::Channel;
    aeron = AeronContext(),
    ENV=ENV
) = serve(Returns(nothing), sm, event_queue; aeron, ENV)
function serve(
    sensorpoll_callback::Base.Callable,
    sm::Hsm.AbstractStateMachine,
    event_queue::Channel;
    aeron = AeronContext(),
    ENV=ENV
)

    pub_status_conf = AeronConfig(
        uri=ENV["PUB_STATUS_URI"],
        stream=parse(Int, ENV["PUB_STATUS_STREAM"]),
    )
    sub_control_conf = AeronConfig(
        uri=ENV["SUB_CONTROL_URI"],
        stream=parse(Int, ENV["SUB_CONTROL_STREAM"]),
    )
    # Prepare 0 or more aeron data input streams
    sub_data_stream_confs = AeronConfig[]
    sub_data_stream_late_data_drop_timer_s = Float64[]
    i = 1
    while haskey(ENV, "SUB_DATA_URI_$i")
        push!(sub_data_stream_confs, AeronConfig(
            uri=ENV["SUB_DATA_URI_$i"],
            stream=parse(Int, ENV["SUB_DATA_STREAM_$i"]),
        ))
        late_data_drop_timer_s = Inf
        if haskey(ENV, "SUB_DATA_LATE_DATA_DROP_SEC_$i")
            late_data_drop_timer_s = parse(Float64, ENV["SUB_DATA_LATE_DATA_DROP_SEC_$i"])
        end
        push!(sub_data_stream_late_data_drop_timer_s, late_data_drop_timer_s)
        i += 1
    end

    GC.enable_logging()

    # Initialize the state machine
    Hsm.transition!(sm, :Top)
    
    # After any command style message is received, we publish our current
    # state out to our output status channel.
    status_report_buffer = zeros(UInt8,2^23) # Pre-allocate a buffer of 8Mb for the largest sized message we plan to see
    status_report_msg = EventMessage(status_report_buffer)
    status_report_msg.name = "state"
    status_report_msg.header.description = ""

    # TODO: we could do after on initialize in the RT loop.
    run(`systemd-notify --ready`, wait=false)

    empty_event_data = view(UInt8[],1:-1)

    start_time = round(UInt64, time()*1e9)

    local sub_control = nothing
    local subs_data_vec = nothing
    local pub_status = nothing
    local subs_data = nothing
    try
        # only subscribe to the data streams *after* we have initialized
        # our block! This will reduce the frame drops when first connecting a new
        # block
        pub_status = Aeron.publisher(aeron, pub_status_conf)
        sub_control = Aeron.subscriber(aeron, sub_control_conf)
        subs_data_vec = Aeron.AeronSubscription[]
        for conf in sub_data_stream_confs
            push!(subs_data_vec, Aeron.subscriber(aeron, conf))
        end
        subs_data = subs_data_vec #tuple(subs_data_vec...)
        # TODO: need a function barrier here so that subs_data is type stable and the
        # for loop unrolls.

        correlationId = 1
        send_status_update!(pub_status, status_report_buffer, sm, correlationId)

        i = 0
        did_work_or_received_data_i=0
        while true
            i += 1

            if !isempty(event_queue)
                did_work_or_received_data_i = time()
                prevstate = Hsm.current(sm)
                Hsm.dispatch!(sm, take!(event_queue), empty_event_data)
                afterstate = Hsm.current(sm)
                if prevstate != afterstate
                    status_report_msg.name = cstatic"state"
                    setargument!(status_report_msg, String(afterstate)) # Note: this allocates on state change.
                    status_report_msg.header.TimestampNs = round(UInt64, time()*1e9)
                    status_report_msg.header.correlationId = event.header.correlationId
                    Aeron.put!(pub_status, view(status_report_buffer, 1:sizeof(status_report_msg)))
                end
            end

            # Process any data
            for (sub,late_data_drop_timer_s) in zip(subs_data, sub_data_stream_late_data_drop_timer_s)
                fragmentsread, data = Aeron.poll(sub)
                if isnothing(data)
                    continue
                end
                did_work_or_received_data_i = time()
                t_arrival_ns = round(UInt64, time()*1e9)
                generic_msg = GenericMessage(data.buffer)
                msg_name = SpidersMessageEncoding.sbemessagename(data.buffer)
                # We check two timing conditions.
                # First, any data must arrive after the service started to be considered
                if generic_msg.header.TimestampNs < start_time
                    @warn "Discarding stale data message (generated before the service started)"# maxlog=10
                    continue
                end
                # Second, if there is a user-specified frame-drop delay, check that the arriving data is fresh enough.
                # This prevents a single late message from cascading into many by letting us catch up.
                # Note: clamp is to prevent any craziness if the clocks eg between computers drift or something.
                delay = Float64(clamp(t_arrival_ns - generic_msg.header.TimestampNs, 0, typemax(Int64)))/1e9
                if delay > late_data_drop_timer_s
                    # TODO: would printing here actually delay us more? :(
                    # @warn "Dropping frame!" generic_msg.header.TimestampNs t_arrival_ns late_data_drop_timer_s
                    status_report_msg.name = cstatic"FrameDrop"
                    setargument!(status_report_msg, delay)
                    status_report_msg.header.TimestampNs = round(UInt64, time()*1e9)
                    status_report_msg.header.correlationId = generic_msg.header.correlationId
                    Aeron.put!(pub_status, view(status_report_buffer, 1:sizeof(status_report_msg)))
                end
                try
                    Hsm.dispatch!(sm, :Data, data.buffer)
                catch err
                    @error "Error in Data dispatch to state machine" exception=(err, catch_backtrace())
                    Hsm.transition!(sm, :Error)
                    status_report_msg.name = cstatic"state"
                    setargument!(status_report_msg, String(Hsm.current(sm))) # Note: this allocates on state change.
                    status_report_msg.header.TimestampNs = round(UInt64, time()*1e9)
                    status_report_msg.header.correlationId = generic_msg.header.correlationId
                    Aeron.put!(pub_status, view(status_report_buffer, 1:sizeof(status_report_msg)))
                end
            end

            # Now check control channel
            bytesread, data = Aeron.poll(sub_control)
            if !isnothing(data)
                did_work_or_received_data_i = time()
                msg_name = SpidersMessageEncoding.sbemessagename(data.buffer)
                
                generic_msg = GenericMessage(data.buffer)
                if generic_msg.header.TimestampNs <= start_time
                    @warn "Discarding stale control/event message" maxlog=10
                    continue
                end

                # A command / event to process (after the next commit is received)
                if msg_name == :EventMessage

                    # We can have multiple event messages concatenated together.
                    # In this case, we apply each sequentually in one go. 
                    # This allows changing multiple parameters "atomically" between
                    # loop updates.
                    last_ind = 0
                    while last_ind < length(data.buffer) # TODO: don't fail if there are a few bytes left over
                        data_span = @view data.buffer[last_ind+1:end]
                        event = EventMessage(data_span, initialize=false)
                        if event.name == "StatusRequest"
                            # We handle this directly instead of in the state machine
                            # since we have access to pub_status and they don't
                            # In general, the state machine doesn't have to be aware
                            # of the control and status channels
                            send_status_update!(pub_status, status_report_buffer, sm, event.header.correlationId, )
                            last_ind += sizeof(event)
                            continue
                        end

                        event_data = view(data_span, 1:sizeof(event))
                        
                        # Dispatch event
                        event_name = Symbol(event.name)
                        println(event_name)
                        prevstate = Hsm.current(sm)
                        local handled
                        try
                            handled = Hsm.dispatch!(sm, event_name, event_data)
                        catch err
                            @error "Error in event dispatch to state machine" exception=(err, catch_backtrace())
                            handled = false
                        end
                        afterstate = Hsm.current(sm)

                        # TODO: we're still sending acknowledgements even when an event is not handled. Can we use a return value of dispatch!? Update: maybe this is now fixed.
                        # Check we haven't fallen into an error state or error sub-state
                        if handled && !Hsm.ischildof(sm, afterstate, :Error)
                            # Republish this message to our status channel so that senders
                            # can know we have received and dealt with their command
                            # This is a form of *acknowledgement*
                            Aeron.put!(pub_status, event_data)
                        end

                        # If the state has changed, publish our current state. This is so listeners know
                        # what we're doing without having to keep look at our history
                        # of transitions.
                        if prevstate != afterstate
                            status_report_msg.name = cstatic"state"
                            setargument!(status_report_msg, String(afterstate)) # Note: this allocates on state change.
                            status_report_msg.header.TimestampNs = round(UInt64, time()*1e9)
                            status_report_msg.header.correlationId = event.header.correlationId
                            Aeron.put!(pub_status, view(status_report_buffer, 1:sizeof(status_report_msg)))
                        end
                        if !handled
                            break
                        end
                        if !isempty(event_queue)
                            prevstate = Hsm.current(sm)
                            Hsm.dispatch!(sm, take!(event_queue), empty_event_data)
                            afterstate = Hsm.current(sm)
                            if prevstate != afterstate
                                status_report_msg.name = cstatic"state"
                                setargument!(status_report_msg, String(afterstate)) # Note: this allocates on state change.
                                status_report_msg.header.TimestampNs = round(UInt64, time()*1e9)
                                status_report_msg.header.correlationId = event.header.correlationId
                                Aeron.put!(pub_status, view(status_report_buffer, 1:sizeof(status_report_msg)))
                            end
                        end

                        last_ind += sizeof(event)

                    end
                else
                    @warn "unhandled message received" maxlog=1
                end
            end

            # User can also provide a callback for us to call in this loop
            # periodically. This could be used to e.g. poll sensors or a 
            # camera and then feed events
            sensorpoll_callback()

            # if no work done yet, insert only a GC safepoint very occaisionally just in case there is another Julia thread
            # waiting for GC (but there usually shouldn't be in most applications)
            if did_work_or_received_data_i == 0
                if mod(i, 1000) == 0
                    GC.safepoint()
                end
            # If we haven't done any work in 1000 spins, sleep a moment
            # then go back to spinning until we see data
            elseif time() > did_work_or_received_data_i + 50e-6
                # Make sure we don't starve another thread that has to GC
                GC.safepoint()
                # Definition of Libc.systemsleep:
                # systemsleep(s::Real) = ccall(:usleep, Int32, (UInt32,), round(UInt32, s*1e6))
                # println("sleeping", time())
                Libc.systemsleep(100e-6)
                did_work_or_received_data_i = 0
            end 


        end # End while process loop
    finally
        if !isnothing(pub_status)
            close(pub_status)
        end
        if !isnothing(sub_control)
            close(sub_control)
        end
        if !isnothing(subs_data)
            close.(subs_data)
        end
        close(aeron)
    end
end


# Buffer used to report our current state to listners who connect late and want to catch up.
# const status_report_buffer = zeros(UInt8,2^19)
# const status_report_msg = EventMessage(status_report_buffer)
# const status_report_argument_buffer = zeros(UInt8,2^17) # TODO: handle buffer reallocating and moving instead of just allocating a huge size

function send_status_update!(pub_status, status_report_buffer, sm, correlationId, )
    # Send each paramter
    vars_nt = RTCBlock.statevariables(sm)
    # note: statevariables() should be type stable to avoid allocations in this loop.
    for key in keys(vars_nt)
        val = vars_nt[key]
        status_report_msg = EventMessage(status_report_buffer)
        if val isa AbstractArray
            # If the value is an array, we set the argument of the EventMessage to an appropriate ArrayMessage payload
            # Resize message buffer down to zero argument size
            # setargument!(status_report_msg, nothing)
            # status_report_msg.format = SpidersMessageEncoding.ValueFormatMessage
            # status_report_argument_buffer = view(status_report_buffer, sizeof(status_report_msg)+1:sizeof(status_report_buffer))
            # TODO: use existing space in status_report_buffer instead of allocating here.
            status_report_argument_buffer = zeros(UInt8, sizeof(val)+512)
            arr_message = ArrayMessage{eltype(val),ndims(val)}(status_report_argument_buffer)
            arraydata!(arr_message, val)
            # Resize it down to fit
            resize!(status_report_msg.value, sizeof(arr_message))
            setargument!(status_report_msg, arr_message)
        else
            setargument!(status_report_msg, val)
        end
        status_report_msg.name = String(key) # allocates
        status_report_msg.header.TimestampNs = round(UInt64, time()*1e9)
        status_report_msg.header.correlationId = correlationId
        Aeron.put!(pub_status,  view(status_report_buffer, 1:sizeof(status_report_msg)))
    end
    # Send the overall state
    status_report_msg = EventMessage(status_report_buffer)
    state_str = String(Hsm.current(sm)) # allocates
    status_report_msg.name = cstatic"state"
    setargument!(status_report_msg, state_str) # Note: this allocates.
    status_report_msg.header.TimestampNs = round(UInt64, time()*1e9)
    status_report_msg.header.correlationId = correlationId
    # resize!(status_report_buffer, sizeof(status_report_msg))
    Aeron.put!(pub_status, view(status_report_buffer, 1:sizeof(status_report_msg)))
end

end