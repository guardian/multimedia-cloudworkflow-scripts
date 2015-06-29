#!/usr/bin/env ruby

require 'webrick'
require 'awesome_print'
require 'json'
require 'logger'
require 'aws-sdk'

BIND_PORT = 9000
SNS_ARN = "arn:aws:sns:eu-west-1:855023211239:***REMOVED***"
$subscription_arn = ""

class SNSServlet < WEBrick::HTTPServlet::AbstractServlet
  def confirmSubscription(token)
    #sns = AWS::SNS::Client.new(region: 'eu-west-1')
    response = @sns.confirm_subscription({topic_arn: SNS_ARN,token: token})
    ap response
    $subscription_arn = response[:subscription_arn]
  end

  def initialize(server,*options)
    @sns = AWS::SNS::Client.new(region: 'eu-west-1')
  end

  def do_POST(request,response)
    puts "Got HTTP post"
    ap request.body
    @logger = Logger.new(STDERR)
    
    begin
      data=JSON.parse(request.body)
      
      if(data['Type']=="SubscriptionConfirmation")
	self.confirmSubscription(data['Token'])
      end
      response.status = 200
      response['Content-Type'] = 'text/plain'
      response.body = "OK\r\n"
    rescue JSON::ParserError=>e
      @logger.error("Invalid JSON data passed: %p #{e.message}" % request.body)
      response.status = 400
      response['Content-Type'] = 'text/plain'
      response.body = "Invalid JSON\r\n"
    rescue StandardError=>e
      @logger.error("Unable to process data: #{e.message}")
      response.status = 500
      response['Content-Type'] = 'text/plain'
      response.body = "Server error\r\n"
    end
    
  end
  
end

#START MAIN
sns = AWS::SNS::Client.new(region: 'eu-west-1')
server = WEBrick::HTTPServer.new(:Port=>BIND_PORT)

server.mount('/messages',SNSServlet)
trap 'INT' do
  puts "Caught interrupt, shutting down..."
  server.shutdown
end

trap 'TERM' do
  puts "Caught terminate, shutting down..."
  server.shutdown
end

at_exit do
  sns.unsubscribe(:subscription_arn=>$subscription_arn)
end
response = sns.subscribe({
	topic_arn: SNS_ARN,
	protocol: "http",
	endpoint: "http://ec2-54-72-17-35.eu-west-1.compute.amazonaws.com:#{BIND_PORT}/messages"
})
#response = sns.confirm_subscription(:topic_arn=>"arn:aws:sns:eu-west-1:855023211239:***REMOVED***",
#	:token=>response[:response_metadata][:request_id])
#ap response
#confirmation message comes from the message
server.start
