module ManageIQ
  module Messaging
    class Client
      require 'manageiq/messaging/stomp/client'

      # Open or create a connection to the message broker
      # @param type [String or Symbol] client type, available choices are:
      #   :Stomp
      #   :AMQP
      #   :MiqQueue
      # @param options [Hash] the connection options
      # @return [Client, nil] the client object if no block is given
      #   The optional block supply {|client| block }. The client will
      #   be automatically closed when the block terminates
      #
      # Avaiable type:
      def self.open(type = :Stomp, options)
        client = Object.const_get("ManageIQ::Messaging::#{type}::Client").new(options)
        return client unless block_given?

        begin
          yield client
        ensure
          client.close
        end
        nil
      end

      # Publish to a message to a queue. The message will be delivered to only one
      # subscriber.
      # @param options [Hash] the message attributes. Expected keys are:
      #   :service    (service and affinity are used to determine the queue name)
      #   :affinity   (optional)
      #   :class_name (optional)
      #   :message (e.g. method_name or message type)
      #   :payload (user defined structure, following are some examples)
      #     :instance_id
      #     :args
      #     :miq_callback
      #   :sender    (optional, identify the sender)
      #   <other queue options TBA>
      #
      # Optionally a call back block {|response| block} can be provided to wait on
      # the consumer to send an acknowledgment.
      def publish_message(options, &block)
        assert_options(options, [:message, :service])

        publish_message_impl(options, &block)
      end

      # Publish multiple messages to a queue.
      # An aggregate version of `#publish_message `but for better performance
      # All messages are sent in a batch
      #
      # @param messages [Array] a collection of options for `#publish_message`
      def publish_messages(messages)
        publish_messages_impl(messages)
      end

      # Subscribe to receive messages from a queue
      #
      # @param options [Hash] attributes to configure how to receive messages.
      #  Available keys are:
      #   :service  (service and affinity are used to determine the queue)
      #   :affinity (optional)
      #   :limit    (optional, receives up to limit messages into the buffer)
      #
      # A callback block {|messages| block} needs to be provided to consume the
      # messages. Example
      #   subscribe_message(options) do |messages|
      #     messages.collect do |msg|
      #       # from msg you get
      #       msg.sender
      #       msg.message
      #       msg.payload
      #       msg.ack_ref (used to ack the message)
      #
      #       client.ack(msg.ack_ref)
      #       # process
      #       result # a result sent back to sender if expected
      #     end
      #   end
      #
      # @note The subscriber MUST ack each message independently in the callback
      # block. It can decide when to ack according to whether a message can
      # be retried. Ack the message in the beginning of processing if the
      # message is not re-triable; otherwise ack it after the message is done.
      # Any un-acked message will be redelivered to next subscriber AFTER the
      # current subscriber disconnects normally or abnormally (e.g. crashed).
      # Make sure a message is properly acked whatever strategy you take.
      #
      # To ack a message call `ack(msg.ack_ref)`
      def subscribe_messages(options, &block)
        raise "A block is required" unless block_given?
        assert_options(options, [:service])

        subscribe_messages_impl(options, &block)
      end

      # Subscribe to receive from a queue and run each message as a background job.
      # @param options [Hash] attributes to configure how to receive messages
      #   :service  (service and affinity are used to determine the queue)
      #   :affinity (optional)
      #
      # This subscriber works only if the incoming message includes the class_name option
      #
      # Background job assumes each job is not re-triable. It will ack as soon as a request
      # is received
      def subscribe_background_job(options)
        assert_options(options, [:service])

        subscribe_background_job_impl(options)
      end

      # Publish a message as a topic. All subscribers will receive a copy of the message.
      # @param options [Hash] the message attributes. Expected keys are:
      #   :service   (service is used to determine the topic address)
      #   :event     (event name)
      #   :payload   (user defined structure that describes the event)
      #   :sender    (optional, identify the sender)
      #   <other queue options TBA>
      #
      def publish_topic(options)
        assert_options(options, [:event, :service])

        publish_topic_impl(options)
      end

      # Subscribe to receive topic type messages.
      # @param options [Hash] attributes to configure how to receive messages
      #   :service     (service is used to determine the topic address)
      #   :persist_ref (optional, client needs to be have client_ref to use this feature)
      #
      # Persisted event: In order to consume events missed during the period when the client is
      # offline, the subscriber needs to be reconnect always with the same client_ref and persist_ref
      #
      # A callback {|sender, event, payload| block } needs to be provided to consume the topic
      #
      def subscribe_topic(options, &block)
        raise "A block is required" unless block_given?
        assert_options(options, [:service])

        subscribe_topic_impl(options, &block)
      end

      Struct.new("ManageIQ_Messaging_ReceivedMessage", :sender, :message, :payload, :ack_ref)

      private

      def logger
        ManageIQ::Messaging.logger
      end

      def assert_options(options, keys)
        keys.each do |key|
          raise ArgumentError, "options must contain key #{key}" unless options.key?(key)
        end
      end
    end
  end
end

