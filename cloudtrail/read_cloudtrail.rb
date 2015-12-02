#!/usr/bin/env ruby

require 'elasticsearch'
require 'json'
require 'awesome_print'
require 'date'
require 'trollop'
require 'logger'
require 'aws-sdk-resources'
require 'zlib'

INDEXNAME='cloudtrail'
TYPENAME="event"
$logger=Logger.new(STDOUT)

class ElasticIndexer
  def initialize(client: nil,autocommit: 0)
    @records = []
    @autocommit_threshold = autocommit
    if client
      @client=client
    else
      @client=Elasticsearch::Client.new()
    end
  end #def initialize

  def flatten_hash(h)
    #print "flatten_hash got:"
    #ap(h)
    newhash={}
    h.each do |k,v|
      if v.is_a?(Hash)
        v.each {|subkey,subval|
          if subval.is_a?(Hash)
            newhash[subkey]=flatten_hash(subval)
          else
            newhash[subkey]=subval
          end
        }
        #h.delete(k)
      else
        newhash[k]=v
      end
    end
    #print "flatten_hash returned:"
    #ap(h)
    return h
  end #def flatten.hash
  
  def add_record(rec)
    #$logger.info("adding record")
    if rec.is_a?(Hash)
      @records << self.flatten_hash(rec)
    else
      @records << rec
    end
    if @records.length > @autocommit_threshold
      self.commit
    end
  end #def add_record
  
  def commit
    actions = []
    $logger.info("Committing to index #{INDEXNAME}...")
    @records.each do |rec|
      actions << { index: {
        _index: INDEXNAME,
        _type: TYPENAME,
        data: rec
      }}
    end
    @client.bulk(body: actions)
    @records = []
  end #def commit
  
end #class ElasticIndexer

def find_numerics(rec)
  rtn=rec
  rec.each {|k,v|
    if v.is_a?(Hash)
      rtn[k] = find_numerics(v)
    else
      begin
        rtn[k] = int(rec[k])
      rescue
        begin
          rtn[k] = float(rec[k])
        rescue
        end
      end
    end
  }
  return rtn
end

def reformat_event(rec)
  rtn=rec
  if rec['eventTime']
    rtn['eventTime'] = DateTime.rfc3339(rec['eventTime'])
  end

  rtn
  #rtn = find_numerics(rec)
end

def download_from_s3(bucket: nil,key: nil)
  if bucket==nil
    raise ArgumentError, "You must supply a bucket name to download_from_s3"
  end
  if key==nil
    raise ArgumentError, "You must specify a key (aka, filepath) to download_from_s3"
  end
  
  $logger.debug("Attempting to download #{key} from #{bucket}...")
  s3 = Aws::S3::Client.new(region: $opts.region)
  b = Aws::S3::Bucket.new(bucket, client: s3)
  if not b.exists?
    raise ArgumentError,"The bucket #{bucket} does not exist."
  end
  
  if not b.object(key).exists?
    $logger.info("#{key} does not exist in #{bucket}, so assuming it's url-encoded")
    key = URI.unescape(key)
    #raise ArgumentError,"The object #{key} does not exist in the bucket #{bucket}"
  end
  
  #download to memory
  b.object(key).get().body
end

def download_and_process(b,object_key, indexer: nil)
  n=0
  compressed_data = download_from_s3(bucket: b, key: object_key)
  
  uncompressed_data = Zlib::GzipReader.new(compressed_data).read
  final_data = JSON.parse(uncompressed_data)
  
  final_data['Records'].each {|rec|
    processed_rec = reformat_event(rec)
    indexer.add_record(processed_rec)
    ap(processed_rec)
    n+=1
  }
  n
end

#START MAIN
$opts = Trollop::options do
  opt :elasticsearch, "Location of elasticsearch cluster to communicate with. Specify multiple hosts separated by commas.", :type=>:string, :default=>"localhost"
  opt :queueurl, "URL of the Amazon SQS queue to listen to", :type=>:string
  opt :region, "AWS region to operate in", :type=>:string, :default=>"eu-west-1"
  opt :reindex, "Perform a complete re-index based on the s3 bucket listed", :type=>:boolean
  opt :bucket, "S3 bucket to re-index from, if performing a re-index", :type=>:string
end

ets = Elasticsearch::Client.new(hosts: $opts.elasticsearch.split(/,\s*/),log: true)
ets.cluster.health
indexer = ElasticIndexer.new(client: ets,autocommit: 500)

if $opts[:reindex]
  $logger.info("Attempting to perform a full re-index from #{$opts.bucket}")
  s3 = Aws::S3::Client.new(region: $opts.region)
  b = Aws::S3::Bucket.new($opts.bucket, client: s3)
  if not b.exists?
    raise ArgumentError,"The bucket #{bucket} does not exist."
  end
  
  n=0
  b.objects.each {|obj|
    begin
      n+=download_and_process(obj.bucket_name,obj.key,indexer: indexer)
    rescue StandardError=>e
      $logger.warn(e)
    end
  }
  $logger.info("Re-index complete, re-indexed #{n} records")
end

if $opts[:queueurl]==nil
  $logger.error("You need to specify a queue to listen to using --queueurl")
  exit(1)
end



#puts "Reading #{ARGV[0]}..."
#data={}
#File.open(ARGV[0]) do |f|
#  data=JSON.parse(f.read())
#end

c=Aws::SQS::Client.new(region: $opts.region)

Aws::SQS::QueuePoller.new($opts[:queueurl], {:client=>c}).poll do |msg|
  n=0
  begin
    #puts msg
    $logger.debug("Got message #{msg.body}")
    content=JSON.parse(msg.body)
    
    message_content = JSON.parse(content['Message'])
    
    message_content['s3ObjectKey'].each {|object_key|
      n+=download_and_process(message_content['s3Bucket'],object_key,indexer: indexer)
    }
  rescue StandardError=>e
    $logger.error(e)
    $logger.error(e.traceback)
  end
  indexer.commit
  puts "Done. Processed #{n} records."
end

#n=0
#data['Records'].each {|rec|
#  processed_rec = reformat_event(rec)
#  indexer.add_record(processed_rec)
#  ap(processed_rec)
#  n+=1
#}


