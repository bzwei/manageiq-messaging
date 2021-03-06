module ManageIQ
  module Messaging
    class BackgroundJob
      include Common

      def self.subscribe(client, options)
        assert_options(options, [:service])

        queue_name, headers = queue_for_subscribe(options)

        client.subscribe(queue_name, headers) do |msg|
          client.ack(msg)
          begin
            assert_options(msg.headers, ['class_name', 'message_type'])

            msg_options = decode_body(msg.headers, msg.body)
            msg_options = {} if msg_options.empty?
            logger.info("Processing background job: queue(#{queue_name}), job(#{msg_options.inspect}), headers(#{msg.headers})")
            run_job(msg_options.merge(:class_name => msg.headers['class_name'], :method_name => msg.headers['message_type']))
            logger.info("Background job completed")
          rescue Timeout::Error
            logger.warn("Background job timed out")
            if Object.const_defined?('ActiveRecord::Base')
              begin
                logger.info("Reconnecting to DB after timeout error during queue deliver")
                ActiveRecord::Base.connection.reconnect!
              rescue => err
                logger.error("Error encountered during <ActiveRecord::Base.connection.reconnect!> error:#{err.class.name}: #{err.message}")
              end
            end
          end
        end
      end

      def self.run_job(options)
        assert_options(options, [:class_name, :method_name])

        instance_id = options[:instance_id]
        args = options[:args]
        miq_callback = options[:miq_callback]

        obj = Object.const_get(options[:class_name])
        obj = obj.find(instance_id) if instance_id

        msg_timeout = 600 # TODO: configurable per message
        Timeout.timeout(msg_timeout) do
          obj.send(options[:method_name], *args)
        end

        run_job(miq_callback) if miq_callback
      end
      private_class_method :run_job
    end
  end
end
