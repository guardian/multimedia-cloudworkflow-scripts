#!/usr/bin/env ruby

require 'aws-sdk'

class CDSResponder
attr_accessor :url
attr_accessor :isexecuting

def initialize(arn,routename,arg)
	@routename=routename
	@cdsarg=arg
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

sqs=AWS::SQS.new(:region=>'eu-west-1');
ddb=AWS::DynamoDB.new(:region=>'eu-west-1');

table=ddb.tables['workflowmaster-cds-responder']
table.hash_key = ['queue-arn',:string]

table.items.each do |item|
        puts item.hash_value
        item.attributes.each_key do |key|
                puts "\t#{key} => #{item.attributes[key]}\n";
        end
	responder=CDSResponder.new(item.attributes['queue-arn'],item.attributes['route-name'],"--input-"+item.attributes['input-type']);
	puts responder.url
	responder.join
end

