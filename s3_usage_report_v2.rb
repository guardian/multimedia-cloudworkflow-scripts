#!/usr/bin/env ruby
require 'aws-sdk'
require 'csv'

def scan_bucket(table,bucket)

total_size=0
objects_scanned=0
bucket.objects.each do |obj|
	#csv << [ obj.key, obj.content_type, obj.content_length,obj.last_modified ]
	total_size+=obj.content_length
	objects_scanned+=1
	running_total=total_size/1024**3
	print "\t\t#{objects_scanned} objects scanned, running total of #{running_total}Gb...\r";
end
total_size=total_size/1024**3

table.items.put(:entity=>bucket.name,
		:time=>DateTime.now.strftime("%d %B %Y, %H:%M:%S"),
		:objects=>objects_scanned,
		:total_gb=>total_size)

#csv << [ "Total size of #{bucket.name}: #{total_size} Gb" ]
#csv << [ "----------------------------------------------" ]
end

#START MAIN
$s3=AWS::S3.new
$ddb=AWS::DynamoDB.new(:region=>'eu-west-1')	#FIXME: should read default from ~/.aws/config

table = $ddb.tables['s3_usage_meta']
table.hash_key = [ :entity, :string]

begin

#CSV.open("s3_report.csv","wb") do |csv|
	timestring=DateTime.now.strftime("%d %B %Y, %H:%M:%S")
#	csv << [ "S3 usage report compiled at #{timestring}" ]
	print "Scanning available buckets...\n";
	$s3.buckets.each do |bucket|
		print "\t#{bucket.name}\n";
		begin
		#csv << [ "Bucket name: #{bucket.name}" ]
		scan_bucket(table,bucket)
		print "\n";	
		rescue AWS::S3::Errors::AccessDenied=>e
			puts "Warning: Access denied to bucket #{bucket.name}"
		end
	end
#end

rescue AWS::S3::Errors::AccessDenied=>e
   puts "Warning: #{e.message}:\n\t#{e.http_request.headers.to_hash.to_s}\n\n#{e.http_response.body}"

end
