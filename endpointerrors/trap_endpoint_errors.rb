#!/usr/bin/env ruby

require 'json'
require 'awesome_print'
require 'aws-sdk-core'
require 'uri'
require 'cgi'
require 'fileutils'

#global config
$csEnd = '***REMOVED***'
#document endpoint for cloudsearch domain
$graveyard = "/mnt/trap_endpoint_errors/graveyard"
#end

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
	ap @docs
	puts jsondata
	searchdomain.upload_documents(documents: jsondata,content_type: 'application/json')
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
if(ARGV.length <1)
	puts "Processes error reports from the interactive endpoint"
	puts "Usage: trap_endpoint_errors {report.json}"
	exit 1
end

begin
	rawdata = IO.read(ARGV[0])
	msgdata = JSON.parse(rawdata)
	ap msgdata

	reportdata = JSON.parse(msgdata['Message'])
	reportdata['timestamp'] = msgdata['Timestamp']

	ap reportdata

	db = DocumentBatch.new
	db.addDoc(reportdata,msgdata['MessageId'])

	$searchDomain = Aws::CloudSearchDomain::Client.new(endpoint: $csEnd)
	db.commit($searchDomain)

	reportdata = breakdown_report_data(reportdata)
	if reportdata['detail']['query_args']
		args = CGI::parse(reportdata['detail']['query_args'])
	else
		args = []
	end
	ap args
	File.unlink(ARGV[0])
rescue StandardError=>e
	puts "-ERROR: #{e.message}"
	puts e.backtrace
	FileUtils.mkdir_p($graveyard)
	FileUtils.mv(ARGV[0],$graveyard)
end
