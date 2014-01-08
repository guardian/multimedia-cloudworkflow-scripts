#!/usr/bin/env ruby

require 'aws-sdk'

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

	@routefile=DownloadRoute()
	
	@commandline="cds_run --route \"#{@routefile}\" #{@cdsarg}="

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

def ThreadFunc

while @isexecuting do
	@q.receive_message { |msg|
		#puts "Received message:\n";
		#puts "\t#{msg.body}\n";
		triggerfile=OutputTriggerFile(msg.body,msg.id)
		cmd=@commandline + triggerfile + " --logging-id=#{msg.id}"
		system(cmd)
		File.delete(triggerfile)
	}
end

end

def join
@threadref.join
File.delete(@routefile)
end

end

begin

sqs=AWS::SQS.new(:region=>'eu-west-1');
ddb=AWS::DynamoDB.new(:region=>'eu-west-1');

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

