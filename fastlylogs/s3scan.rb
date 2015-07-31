#!/usr/bin/env ruby
require 'aws-sdk-core'
require 'aws-sdk-resources'
require 'trollop'
require 'logger'
require 'json'

#START MAIN
$logger=Logger.new(STDOUT)

$opts = Trollop::options do
  opt :bucket, "S3 bucket to scan", :type=>:string
  opt :prefix, "Only scan objects with this prefix (aka, in this folder)", :type=>:string
  opt :queue, "SQS queue to output to", :type=>:string
  opt :region, "AWS region to operate in", :type=>:string, :default=>"eu-west-1"
end

if not $opts.bucket
  puts "You must specify a bucket name on the commandline using the --bucket option"
end

s3 = Aws::S3::Client.new(region: $opts.region)
b = Aws::S3::Bucket.new($opts.bucket, client: s3)

sqs = nil
if $opts.queue
  sqs = Aws::SQS::Client.new(region: $opts.region)
end

if not b.exists?
  $logger.error("Bucket #{$opts.bucket} does not exist")
  exit(1)
end

b.objects(prefix: $opts.prefix).each {|objectsummary|
  if sqs
    payload = JSON.generate({
      'Event' => 'new',
      'Bucket' => b.name,
      'Key' => objectsummary.key
    })
    sqs.send_message(queue_url: $opts.queue,message_body: payload)
  end
  
  puts "#{objectsummary.key} in #{b.name}" 
}
