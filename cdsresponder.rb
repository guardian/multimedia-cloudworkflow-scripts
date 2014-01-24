#!/usr/bin/env ruby

require 'aws-sdk'
require 'json'
require 'optparse'

class ConfigFile
attr_accessor :var

def initialize(filename)

unless File.exists?(filename)
	raise "Requested configuration file #{filename} does not exist."
end

File.open(filename,"r"){ |f|
	f.each_line { |line|
		row=line.match(/^(?<name>[^=]+)=(?<value>.*)$/)
		@var[row['name']]=row['value']
	}
}

end

end

class FinishedNotification
attr_accessor :exitcode
attr_accessor :log
attr_accessor :routename

def initialize(routename,exitcode,log)
	@routename=routename
	@exitcode=exitcode
	@log=log
end

def to_json
	hash={}
	self.instance_variables.each do |var|
		hash[var]=self.instance_variable_get var
	end
	hash.to_json
end

end

class CDSResponder
attr_accessor :url
attr_accessor :isexecuting

def initialize(arn,routename,arg,notification)
	@routename=routename
	@cdsarg=arg
	@notification_arn=notification
	matchdata=arn.match(/^arn:aws:sqs:([^:]*):([^:]*):([^:]*)/);
	@region=matchdata[1];
	@acct=matchdata[2];
	@name=matchdata[3];
	@url="https://sqs.#{@region}.amazonaws.com/#{@acct}/#{@name}";
	@sqs=AWS::SQS::new(:region=>'eu-west-1');
	@q=@sqs.queues[@url]
	@isexecuting=1;

	if notification!=nil 
		@sns=AWS::SNS.new(:region=>'eu-west-1')
		@notification_topic=@sns.topics[notification]
	end

	@threadref=Thread.new {
		ThreadFunc();
	}
end

def GetUniqueFilename(path)
filebase=@routename.gsub(/[^\w\d]/,"_")

filename=path+'/'+filebase+".xml"

n=0;
while(Pathname(filename).exist?) do
	n=n+1
	filename=path+'/'+filebase+"-"+n.to_s()+".xml"
end
filename
end

def GetRouteContent
ddb=AWS::DynamoDB.new(:region=>'eu-west-1')
table=ddb.tables['workflowmaster-cds-routes']
table.hash_key=[ :routename,:string ]
item=table.items[@routename]
item.attributes['content']

end

#Download the route name given from the DynamoDB table and return the local filename
def DownloadRoute
filename=GetUniqueFilename('/etc/cds_backend/routes')

puts "Got filename #{filename} for route file\n"

File.open(filename, 'w'){ |f|
	f.write(GetRouteContent())
}
filename
end

#Output the message as a trigger file
def OutputTriggerFile(contents,id)

File.open(id+".xml", 'w'){ |f|
	f.write(contents)
}
id+".xml"
end

def GetLogfile(name)
filename = @logpath+"/"+name+".log"
File.open(@logpath+"/"+name+".log", 'r'){ |f|
	contents=f.read()
}
contents
rescue
	puts "Unable to read log from filename\n"
end

def ThreadFunc

while @isexecuting do
	@q.receive_message { |msg|
	begin #start a block to catch exceptions
		#puts "Received message:\n";
		#puts "\t#{msg.body}\n";
		@routefile=DownloadRoute()
		@commandline="cds_run --route \"#{@routefile}\" #{@cdsarg}="

		triggerfile=OutputTriggerFile(msg.body,msg.id)
		#FIXME: should not be using static db credentials! should be in dynamodb somewhere.
		cmd=@commandline + triggerfile + " --logging-db=***REMOVED*** --db-host=***REMOVED*** --db-login=cdslogger --db-pass=***REMOVED*** --logging-id=#{msg.id}"
		system(cmd)

		msg=FinishedNotification.new(@routename,$?.exitstatus,GetLogfile(msg.id))
		@notification_topic.publish(msg.to_json)
	
	rescue Exception => e
		puts e.message
		puts e.backtrace.inspect

	ensure	
		File.delete(triggerfile)
		File.delete(@routefile)
	end	#end block to catch exceptions
	}
end

end

def join
@threadref.join
#File.delete(@routefile)
end

end

begin

#Process any commandline options
options={:configfile=>'/etc/cdsresponder.conf',:region='eu-west-1'}
OptionParser.new do |opts|
	opts.banner="Usage: cdsresponder.rb [--config=/path/to/config.file] [--region=aws-region]"

	opts.on("-c","--config [CONFIGFILE]", "Path to the configuration file.  This should contain the following:",
				"\tconfiguration-table={dynamodb table to use for configuration}",
				"\troutes-table={dynamodb table to use for the routes content}",
				"\tregion={AWS region to use for SQS and DynamoDB"
				"\taccess-key={AWS access key} [Optional; default behaviour is to attempt connection via AWS roles"
				"\tsecret-key={AWS secret key} [Optional; as above") do |cfg|
		options.configfile=cfg
	end

	opts.on("-r","--region [REGION]","AWS region to connect to") do |r|
		options.region=r
	end

end

begin
	cfg=ConfigFile.new(options.configfile)
rescue Exception=>e
	
end

sqs=AWS::SQS.new(:region=>options.region);
ddb=AWS::DynamoDB.new(:region=>options.region);

table=ddb.tables['workflowmaster-cds-responder']
table.hash_key = ['queue-arn',:string]

responders = Array.new;

table.items.each do |item|
        begin
	puts item.hash_value
        item.attributes.each_key do |key|
                puts "\t#{key} => #{item.attributes[key]}\n";
        end
	for i in 1..item.attributes['threads']
		responder=CDSResponder.new(item.attributes['queue-arn'],item.attributes['route-name'],"--input-"+item.attributes['input-type'],item.attributes['notification'])
		responders.push(responder);
	end

	rescue
		puts "Responder failed to start up for this queue\n";
		next 
	puts responder.url
	end
end

responders.each {|resp|
	resp.join
}

#rescue
#	print "Terminating program...\n"
#ensure
#	responders.each {|resp|
#		resp.isexecuting=0
#		resp.join
#	}
end

