#!/usr/bin/env ruby

require 'webrick'
require 'awesome_print'
require 'json'
require 'logger'
require 'aws-sdk'
require 'open-uri'
require 'logger'
require 'trollop'

BIND_PORT = 9000
SNS_ARN = "arn:aws:sns:eu-west-1:855023211239:***REMOVED***"
$subscription_arn = ""
#$logfile = "/var/log/webrick_sns_test.log"

class EC2Meta
  META_URI = "http://169.254.169.254/latest/meta-data"
  
  def initialize(timeout: 5)
    if $logfile.nil?
      @logger = Logger.new(STDOUT)
    else
      @logger = Logger.new($logfile)
    end
    @logger.level = Logger::DEBUG
    @timeout=timeout
    
    timeout(timeout) do
      @allowed_options = self._inspect(nil,recurse: false)
    end
  end
  
  def loglevel(level)
    @logger.level = level
  end
  
  def _inspect(subsection,recurse: false)
    if subsection != nil
      uristring = [META_URI,subsection].join('/')
    else
      uristring = META_URI
    end
    
    uristring.gsub!(/([^:])\/\//,'\1/')
    
    @logger.debug("Opening #{uristring}...")
    open(uristring) do |f|
      data=f.read
      options=data.split("\n")
      @logger.debug("Got #{options}")
      return options if(not recurse)

      options.each {|opt|
	next if(opt.nil?)
	next if(opt.is_a?(Array))
	if opt.end_with?('/')
	  options << self._inspect([subsection,opt].join('/'),recurse: true)
	end
      } #options.each
      options
    end #open(uristring)
  end #def _inspect
  
  def allowed_options
    @allowed_options
  end
  
  def lookup(optionname)
    if not @allowed_options.include?(optionname)
      raise ArgumentError, "#{optionname} is not a valid metadata request"
    end
    
    uristring = [META_URI,optionname].join('/')
    uristring.gsub!(/([^:])\/\//,'\1/')
    
    open(uristring) do |f|
      f.read
    end
  end #def lookup

end #class EC2Meta

class SNSServlet < WEBrick::HTTPServlet::AbstractServlet
  def confirmSubscription(token)
    #sns = AWS::SNS::Client.new(region: 'eu-west-1')
    response = @sns.confirm_subscription({topic_arn: SNS_ARN,token: token})
    ap response
    $subscription_arn = response[:subscription_arn]
  end

  def initialize(server,*options)
    super
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
logger = Logger.new(STDOUT)

#try to determine our public-facing address using EC2 metadata
logger.info("Attempting to determine own public hostname from EC2")
begin
  m = EC2Meta.new
  my_address=m.lookup('public-hostname')
rescue Timeout::Error
  logger.warn("Unable to contact EC2 metadata instance server. Maybe this is not running in EC2?")
  my_address=`hostname`.rstrip!
rescue StandardError=>e
  logger.error("Unable to determine public hostname from EC2 metadata: #{e.message}")
  logger.error(e.backtrace)
  my_address=`hostname`.rstrip!
end

opts = Trollop::options do
  opt :my_hostname, "Public facing hostname that SNS can send messages to", :type=>:string, :default=>my_address
  opt :port, "Available port number to start up local server to receive messages from SNS", :type=>:integer, :default=>BIND_PORT
end

sns = AWS::SNS::Client.new(region: 'eu-west-1')
#set up HTTP server
server = WEBrick::HTTPServer.new(:Port=>BIND_PORT)
server.mount('/messages',SNSServlet)

#trap signals to shut down cleanly on INT (ctrl-C) or TERM (kill)
trap 'INT' do
  puts "Caught interrupt, shutting down..."
  server.shutdown
end

trap 'TERM' do
  puts "Caught terminate, shutting down..."
  server.shutdown
end

#ensure that when we exit we unsubscribe from the topic
at_exit do
  sns.unsubscribe(:subscription_arn=>$subscription_arn)
end

my_endpoint = "http://#{opts.my_hostname}:#{opts.port}/messages"
logger.info("Requesting subscription to own endpoint at #{my_endpoint}")

response = sns.subscribe({
	topic_arn: SNS_ARN,
	protocol: "http",
	#endpoint: "http://ec2-54-72-17-35.eu-west-1.compute.amazonaws.com:#{BIND_PORT}/messages"
	endpoint: my_endpoint
})
#response = sns.confirm_subscription(:topic_arn=>"arn:aws:sns:eu-west-1:855023211239:***REMOVED***",
#	:token=>response[:response_metadata][:request_id])
#ap response
#confirmation message comes from the message
server.start
