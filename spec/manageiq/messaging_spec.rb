require "spec_helper"

describe ManageIQ::Messaging do
  it "has a version number" do
    expect(ManageIQ::Messaging::VERSION).not_to be nil
  end

  it "has a default logger" do
    expect(ManageIQ::Messaging.logger).to be_kind_of(Logger)
  end

  it "accepts a user defined logger" do
    another_logger = Logger.new(STDOUT)
    ManageIQ::Messaging.logger = another_logger
    expect(ManageIQ::Messaging.logger).to eq(another_logger)
  end
end
