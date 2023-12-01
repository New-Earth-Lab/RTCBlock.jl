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
function serve(
    sm::Hsm.AbstractStateMachine;
    aeron = AeronContext()
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
    i = 1
    while haskey(ENV, "SUB_DATA_URI_$i")
        push!(sub_data_stream_confs, AeronConfig(
            uri=ENV["SUB_DATA_URI_$i"],
            stream=parse(Int, ENV["SUB_DATA_STREAM_$i"]),
        ))
        i += 1
    end

    # Initialize the state machine
    Hsm.transition!(sm, :Top)

    # Keep a queue of commands to apply.
    # When commands are received, we queue them up.
    # When a CommitMessage is received, we process all events at once
    # before returning to processing data.
    command_message_queue = zeros(UInt8,0)
    sizehint!(command_message_queue, 2^14)
    
    # After any command style message is received, we publish our current
    # state out to our output status channel.
    status_report_buffer = zeros(UInt8,1024)
    status_report_msg = CommandMessage(status_report_buffer)
    status_report_msg.command = "state"
    status_report_msg.header.description = ""
    resize!(status_report_buffer, sizeof(status_report_msg))

    # Keep a queue of commands to apply.
    # When commands are received, we queue them up.
    # When a CommitMessage is received, we process all events at once
    # before returning to processing data.
    command_message_queue = zeros(UInt8,0)
    sizehint!(command_message_queue, 2^14)

    # After any command style message is received, we publish our current
    # state out to our output status channel.
    status_report_buffer = zeros(UInt8,1024)
    status_report_msg = CommandMessage(status_report_buffer)
    status_report_msg.command = "state"
    status_report_msg.header.description = ""
    resize!(status_report_buffer, sizeof(status_report_msg))

    # TODO: we could do after on initialize in the RT loop.
    run(`systemd-notify --ready`, wait=false)

    local sub_control = nothing
    local subs_data_vec = nothing
    local pub_status = nothing
    try
        # only subscribe to the data streams *after* we have initialized
        # our block! This will reduce the frame drops when first connecting a new
        # block
        pub_status = Aeron.subscriber(aeron, pub_status_conf)
        sub_control = Aeron.subscriber(aeron, sub_control_conf)
        subs_data_vec = Aeron.AeronSubscription[]
        for conf in sub_data_stream_confs
            push!(subs_data_vec, Aeron.subscriber(aeron, conf))
        end
        subs_data = tuple(subs_data_vec...)
        # TODO: need a function barrier here so that subs_data is type stable and the
        # for loop unrolls.

        while true
            # Process any data
            for sub in subs_data
                bytesread, data = Aeron.poll(sub)
                if !isnothing(data)
                    try
                        Hsm.dispatch!(sm, :Data, data.buffer)
                    catch err
                        @error "Error in loop" exception=(err, catch_backtrace())
                        return 1
                    end
                end
            end
            # Now check control channel
            bytesread, data = Aeron.poll(sub_control)
            if isnothing(data)
                continue
            end
            try
                msg_name = SpidersMessageEncoding.sbemessagename(data.buffer)
                
                # TODO: we are going to simplify the commit logic 

                # A command / event to process (after the next commit is received)
                if msg_name == :CommandMessage
                    cmd = CommandMessage(data.buffer)
                    # @info "Command received, queueing" cmd.command
                    # Queue received commands back to back in a buffer.
                    startpos = sizeof(command_message_queue)+1
                    resize!(command_message_queue, sizeof(command_message_queue)+sizeof(cmd))
                    command_message_queue[startpos:end] .= view(data.buffer, 1:sizeof(cmd))
                
                # Commit received, process all events
                elseif msg_name == :CommitMessage
                    # @info "Commit Received"
                    commit_msg = CommitMessage(data.buffer)
                    startpos = 1
                    prevstate = Hsm.current(sm)
                    while startpos < length(command_message_queue)
                        # @info "processing command"
                        cmd_data = @view command_message_queue[startpos:end]
                        cmd = CommandMessage(cmd_data)
                        startpos += sizeof(cmd) # Bump start position for next iteration
                        # Check that the correlation Id matches the commit (otherwise discard)
                        if cmd.header.correlationId != commit_msg.header.correlationId
                            continue
                        end
                        
                        cmd = CommandMessage(buffer)        
                        event_name = Symbol(cmd.command) # TODO: make sure this is fast
                        Hsm.dispatch!(sm, event_name, buffer)
                        
                        afterstate = Hsm.current(sm)
                        # Check we haven't fallen into an error state or error sub-state
                        if ischildof(sm, afterstate, :Error)
                            # Republish this message to our status channel so that senders
                            # can know we have received and dealt with their command
                            # This is a form of *acknowledgement*
                            Aeron.put!(aeron_status_output, cmd_data)
                        end


                        # If the state has changed, publish our current state. This is so listeners know
                        # what we're doing without having to keep look at our history
                        # of transitions.
                        if prevstate != afterstate
                            setargument!(status_report_msg, String(Hsm.current(sm))) # Note: this allocates on state change.
                            status_report_msg.header.TimestampNs = 0 # TODO
                            status_report_msg.header.correlationId = rand(Int64)
                            Aeron.put!(aeron_status_output, status_report_buffer)
                        end

                    end
                    resize!(command_message_queue, 0)
                else
                    @warn "unhandled message received" maxlog=1
                end

            catch err
                @error "Error in loop" exception=(err, catch_backtrace())
                return 1
            end

        end # End while process loop
    finally
        close(aeron)
        if !isnothing(pub_status)
            close(pub_status)
        end
        if !isnothing(sub_control)
            close(sub_control)
        end
        close.(subs_data)
    end
end
end