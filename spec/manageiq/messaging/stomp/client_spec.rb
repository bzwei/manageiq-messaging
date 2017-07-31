require "spec_helper"

describe ManageIQ::Messaging::Stomp::Client do
  let(:raw_client) { double(:stomp_client) }

  subject do
    expect(::Stomp::Client).to receive(:new).and_return(raw_client)
    described_class.new(:host => 'localhost', :port => 1234)
  end

  describe '#initialize' do
    it 'creates a client heartbeating' do
      expect(::Stomp::Client).to receive(:new).with(hash_including(
        :hosts           => [hash_including(:host => 'localhost', :port => 1234, :login => 'me', :passcode => 'secret')],
        :connect_headers => hash_including(:host => 'localhost', :"accept-version"=>"1.2", :"heart-beat"=>"2000,0", :"client-id" => 'my-ref')
      ))

      described_class.new(
        :host       => 'localhost',
        :port       => 1234,
        :username   => 'me',
        :password   => 'secret',
        :client_ref => 'my-ref'
      )
    end

    it 'creates a client without heartbeating' do
      expect(::Stomp::Client).to receive(:new).with(hash_including(:hosts => [hash_including(:host => 'localhost', :port => 1234)]))

      described_class.new(:host => 'localhost', :port => 1234, :heartbeat => false)
    end
  end

  describe '#publish_topic' do
    it 'sends the message to the topic' do
      expect(raw_client).to receive(:publish).with(
        'topic/s',
        'p',
        hash_including(:"destination-type" => 'MULTICAST', :event_type => 'e'))

      subject.publish_topic(:service => 's', :event => 'e', :payload => 'p')
    end
  end

  describe '#subscribe_topic' do
    it 'listens to the topic' do
      expect(raw_client).to receive(:subscribe).with(
        'topic/s',
        hash_including(:"subscription-type" => 'MULTICAST', :ack => 'client', :"durable-subscription-name" => 'ref'))

      subject.subscribe_topic(:service => 's', :persist_ref => 'ref') { |_a, _b, _c| nil }
    end
  end

  describe '#publish_message' do
    it 'sends a regular message to the queue' do
      expect(raw_client).to receive(:publish).with(
        'queue/s.uid',
        'p',
        hash_including(:"destination-type" => 'ANYCAST', :message_type => 'm'))

      subject.publish_message(:service => 's', :message => 'm', :affinity => 'uid', :payload => 'p')
    end

    it 'sends a message without affinity to the queue' do
      expect(raw_client).to receive(:publish).with(
        'queue/s.none',
        'p',
        hash_including(:"destination-type" => 'ANYCAST', :message_type => 'm', :sender => 'me'))

      subject.publish_message(:service => 's', :message => 'm', :payload => 'p', :sender => 'me')
    end

    it 'sends a background job type message to the queue' do
      expect(raw_client).to receive(:publish).with(
        'queue/s.uid',
        "---\n:instance_id: 1\n:args:\n- arg1\n- 2\n",
        hash_including(
          :"destination-type" => 'ANYCAST',
          :message_type       => 'my_method',
          :class_name         => 'MyClass',
          :encoding           => "yaml"))

      subject.publish_message(
        :service    => 's',
        :class_name => 'MyClass',
        :message    => 'my_method',
        :affinity   => 'uid',
        :payload    => { :instance_id => 1, :args => ['arg1', 2] })
    end

    it 'sends a message and waits for response' do
      expect(raw_client).to receive(:publish).with(
        'queue/s.none',
        'p',
        hash_including(:"destination-type" => 'ANYCAST', :message_type => 'm', :correlation_id => anything))
      expect(raw_client).to receive(:subscribe)

      subject.publish_message(:service => 's', :message => 'm', :payload => 'p') { |resp| nil }
    end
  end

  describe '#subscribe_messages' do
    it 'listens to the queue' do
      expect(raw_client).to receive(:subscribe).with(
        'queue/s.uid',
        hash_including(:"subscription-type" => 'ANYCAST', :ack => 'client'))

      subject.subscribe_messages(:service => 's', :affinity => 'uid') { |messages| nil }
    end

    it 'listens to the queue without affinity' do
      expect(raw_client).to receive(:subscribe).with(
        'queue/s.none',
        hash_including(:"subscription-type" => 'ANYCAST', :ack => 'client'))

      subject.subscribe_messages(:service => 's') { |messages| nil }
    end
  end

  describe '#subscribe_background_job' do
    it 'listens to the queue' do
      expect(raw_client).to receive(:subscribe).with(
        'queue/s.uid',
        hash_including(:"subscription-type" => 'ANYCAST', :ack => 'client'))

      subject.subscribe_background_job(:service => 's', :affinity => 'uid')
    end

    it 'listens to the queue without affinity' do
      expect(raw_client).to receive(:subscribe).with(
        'queue/s.none',
        hash_including(:"subscription-type" => 'ANYCAST', :ack => 'client'))

      subject.subscribe_background_job(:service => 's')
    end
  end

  describe '#publish_messages' do
    it 'sends to the queue one by one' do
      expect(raw_client).to receive(:publish).with(
        'queue/s.none',
        'p1',
        hash_including(:"destination-type" => 'ANYCAST', :message_type => 'm', :sender => 'me'))
      expect(raw_client).to receive(:publish).with(
        'queue/s.none',
        'p2',
        hash_including(:"destination-type" => 'ANYCAST', :message_type => 'm', :sender => 'me'))

      subject.publish_messages([
        {:service => 's', :message => 'm', :payload => 'p1', :sender => 'me' },
        {:service => 's', :message => 'm', :payload => 'p2', :sender => 'me' }])
    end
  end
end
