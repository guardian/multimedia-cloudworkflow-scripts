#!/usr/bin/env ruby

require 'aws-sdk'
require 'aws-sdk-core'
require 'trollop'
require 'json'

class CloudSearchS3Committer

def initialize(endpointname)
	@endpointname = endpointname
	@cs = Aws::CloudSearchDomain::Client.new(endpoint: endpointname)
	
	@awaitingCommitList = []
	@awaitingCommitString = "["
	
	@csSizeLimit = 5 * (1024**2)
end #def new

def addItem(name,datahash)
	docData = {
		'type'=> 'add',
		'id'=>name,
		'fields'=>datahash
	}
	@awaitingCommitList << docData
	jsonDoc = docData.to_json
	if(jsonDoc.bytesize + @awaitingCommitString.bytesize >= @csSizeLimit)
		puts "debug: adding current document would push total size to #{jsonDoc.bytesize + @awaitingCommitString.bytesize} which is bigger than #{@csSizeLimit}"
		self.commit()
	end
	puts "debug: current buffer size: #{jsonDoc.bytesize + @awaitingCommitString.bytesize}"
	@awaitingCommitString += jsonDoc + ","
end

#commits current buffer awaiting upload and blanks buffer
def commit()
	@awaitingCommitString.chop!
	@awaitingCommitString += "]"
	puts "debug: buffer awaiting commit:\n";
	puts @awaitingCommitString
	
	begin
		response = @cs.upload_documents(documents: @awaitingCommitString, content_type: 'application/json')
	rescue Aws::CloudSearchDomain::Errors::DocumentServiceException=>e
		begin
			structuredExceptionData = JSON.parse(e.message)
			puts "A DocumentServiceException occurred:"
			ap structuredExceptionData
			puts e.backtrace
			exit(1)
		rescue JSON::JSONError
			puts "A DocumentServiceException occurred which was not parseable:"
			puts e.message
			puts e.backtrace
			exit(2)
		end
	end
	
	@awaitingCommitString = "["
	
	puts "INFO: upload_document request:"
	puts "\tStatus: #{response.status}"
	puts "\tAdded documents: #{response.adds}, deleted documents: #{response.deletes}";
	if(response.warnings and response.warnings.length>0)
		puts "\tWARNINGS:"
		response.warnings.each do |w|
			puts "\t\t#{w.message}"
		end #response.warnings.each
	end #if
end #def commit

end #class CloudSearchCommitter

#START MAIN
bucketname = 'gnm-multimedia-loosearchive'
region = 'eu-west-1'
documentEndpoint = 'https://***REMOVED***.eu-west-1.cloudsearch.amazonaws.com'

$s3=AWS::S3.new(:region=>region)

bucketref=$s3.buckets[bucketname]

unless(bucketref.exists?)
	puts "-ERROR: Bucket #{bucketname} does not exist"
	exit(1)
end

csc= CloudSearchS3Committer.new(documentEndpoint)

bucketref.objects.each do |obj|
	puts "Got #{obj.key}"
	pathComponents = obj.key.split("/")	#S3 always uses / even if we're on Windoze
	data = {
		'key'=>obj.key,
		'size'=>obj.content_length,
		'content_type'=>obj.content_type,
		'last_modified'=>obj.last_modified.to_i,
		'server_encryption'=>obj.server_side_encryption?,
		'path_components'=>pathComponents,
		'filename'=>File.basename(obj.key)
	}
	itemName = ""
	if(pathComponents.length > 2)
		itemName = pathComponents[-2] + '_' + pathComponents[-1]
	else
		itemName = obj.key
	end
	itemName.gsub!(/[^a-zA-Z0-9\-\_\/\#\:\.\;\&\=\?\@\$\+\!\*'\(\)\,\%]/,'_') #ensure that the document name is valid for CloudSearch
	
	csc.addItem(itemName,data)
end #bucketref.objects each

csc.commit()	#ensure anything uncommitted is sent
