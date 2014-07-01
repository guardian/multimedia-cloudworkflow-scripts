#!/usr/bin/env ruby
require 'aws-sdk'
require 'csv'

def scan_bucket(table,bucket)
begin

total_size=0
objects_scanned=0
bucket.objects.each do |obj|
begin
	#csv << [ obj.key, obj.content_type, obj.content_length,obj.last_modified ]
	total_size+=obj.content_length
	objects_scanned+=1
	running_total=total_size/1024**3
	print "\t\t#{objects_scanned} objects scanned, running total of #{running_total}Gb...\r";
rescue AWS::S3::Errors::NoSuchKey=>e
	puts "\nWarning: No such key (#{e.message}): #{obj.key} in bucket #{obj.bucket}"
	next
rescue Exception=>e
	puts "\nWarning: An exception occurred - #{e.message}\n"
	next
end #exception block
end #bucket.objects.each

rescue Exception=>e
	puts "\nWarning: An exception occurred outside the scan loop - #{e.message}\n"

ensure
total_size=total_size/1024**3

table.items.put(:entity=>bucket.name,
		:time=>DateTime.now.strftime("%d %B %Y, %H:%M:%S"),
		:objects=>objects_scanned,
		:total_gb=>total_size)

#csv << [ "Total size of #{bucket.name}: #{total_size} Gb" ]
#csv << [ "----------------------------------------------" ]
end
end

#START MAIN
$s3=AWS::S3.new
$ddb=AWS::DynamoDB.new(:region=>'eu-west-1')	#FIXME: should read default from ~/.aws/config

table = $ddb.tables['s3_usage_meta']
table.hash_key = [ :entity, :string]

while table.status == 'CREATING'
	puts "Waiting for s3_usage_meta table to become ready..."
	sleep(10)
end

begin
threads=Array.new

#CSV.open("s3_report.csv","wb") do |csv|
	timestring=DateTime.now.strftime("%d %B %Y, %H:%M:%S")
#	csv << [ "S3 usage report compiled at #{timestring}" ]
	print "Scanning available buckets...\n";
	$s3.buckets.each do |bucket|
		if(ARGV[0] and not bucket.name.match(ARGV[0]))
			next
		end

		print "\t#{bucket.name}\n";
		begin
		#csv << [ "Bucket name: #{bucket.name}" ]
		threads << Thread.new do
			scan_bucket(table,bucket)
		end
		print "\n";	
		rescue AWS::S3::Errors::AccessDenied=>e
			puts "Warning: Access denied to bucket #{bucket.name}"
		end
	end
#end

threads.each do |thr|
	thr.join
end

rescue AWS::S3::Errors::AccessDenied=>e
   puts "Warning: #{e.message}:\n\t#{e.http_request.headers.to_hash.to_s}\n\n#{e.http_response.body}"

end
