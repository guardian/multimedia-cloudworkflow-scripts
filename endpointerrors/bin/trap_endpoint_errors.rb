#!/usr/bin/env ruby2.0

require 'json'
require 'awesome_print'
require 'aws-sdk-core'
require 'aws-sdk-resources'
require 'uri'
require 'cgi'
require 'fileutils'
require 'logger'

#global config
$queueURL = 'https://sqs.eu-west-1.amazonaws.com/855023211239/GNM_MM_EndpointError'
$csEnd = '***REMOVED***'
$amzRegion = 'eu-west-1'
#document endpoint for cloudsearch domain
$graveyard = "/mnt/trap_endpoint_errors/graveyard"
#end

$log = Logger.new('/var/log/trap_endpoint_errors.log')

if not ENV['AWS_REGION']
	ENV['AWS_REGION'] = $amzRegion
end

class DocumentBatch
def initialize
	@docs = []
end #def initialize

def _compileHash(docdata,hashdata)
	hashdata.each do |k,v|
		if v.is_a?(Hash)
			docdata=self._compileHash(docdata,v)
		elsif v.is_a?(String) or v.is_a?(Array)
			docdata['fields'][k]=v
		elsif v.is_a?(Integer)
			docdata['fields'][k]=v
		else
			docdata['fields'][k]=v.to_s
		end
	end
	#ok this is ugly. Problem is that this comes in as a null - no data provided - but we don't intrinsically know whether thats a zero number or a zero string.
	if not docdata['fields']['total_encodings_searched'].is_a?(Integer)
		docdata['fields']['total_encodings_searched'] = 0
	end
	return docdata
end

def addDoc(hashdata,id)
	docdata = { 'type'=> 'add', 'id'=>id, 'fields' => {} }
	#hashdata.each do |k,v|
	#	if v.is_a?(String) or v.is_a?(Array)
	#	docdata['fields'][k]=v
	#end #hashdata.each
	docdata=self._compileHash(docdata,hashdata)
	@docs << docdata
end #def addDoc

def commit(searchdomain) #searchdomain should be an Aws::CloudSearchDomain::Client
	jsondata = @docs.to_json
	$log.info("Committing #{@docs.count} documents to index #{$csEnd}")
	#ap @docs
	#puts jsondata
	searchdomain.upload_documents(documents: jsondata,content_type: 'application/json')
	$log.info("Commit done")
end

end #class documentBatch

def breakdown_report_data(data)
	query_uri = URI(data['detail']['query_url'])
	#data['detail']['query_host'] = query_uri.host
	data['detail']['query_path'] = query_uri.path
	data['detail']['query_args'] = query_uri.query
	return data
end #def breakdown_report_data

#START MAIN
begin
	#if not Dir.exists?($graveyard)
	#	FileUtils.mkdir_p($graveyard)
	#end
	
	#rawdata = IO.read(ARGV[0])
	#msgdata = JSON.parse(rawdata)
	#ap msgdata
	
	$log.info("Setting up queue poller on #{$queueURL}")
	poller = Aws::SQS::QueuePoller.new($queueURL)
	$log.info("Setting up CloudSearch connection to #{$csEnd}")
	$searchDomain = Aws::CloudSearchDomain::Client.new(endpoint: $csEnd)
	
	$log.info("Waiting for messages...")
	poller.poll(max_number_of_messages:10) do |msgbatch|
		db = DocumentBatch.new
		
		$log.info("Processing batch of #{msgbatch.count} messages")
		msgbatch.each {|msgdata|
			reportdata = JSON.parse(msgdata['Message'])
			reportdata['timestamp'] = msgdata['Timestamp']
			$log.debug("#{reportdata}")
			db.addDoc(reportdata,msgdata['MessageId'])
			reportdata = breakdown_report_data(reportdata)
			if reportdata['detail']['query_args']
				args = CGI::parse(reportdata['detail']['query_args'])
			else
				args = []
			end
			$log.debug("#{args}")
		}
		$log.info("Committing...")
		db.commit($searchDomain)
		$log.info("Done.")
	end #poller.poll
	#File.unlink(ARGV[0])
rescue StandardError=>e
	$log.error(e.message)
	$log.error(e.backtrace)
end
