#!/usr/bin/env ruby

require 'awesome_print'
require 'date'
require 'geoip'
require 'logger'
require 'elasticsearch'
require 'trollop'
require 'aws-sdk-resources'
require 'aws-sdk-core'
require 'json'
require 'base64'

INDEXNAME='fastlylogs'
TYPENAME="log"
LOGFILE = "/var/log/fastly_log_chopper.log"


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
  
  def add_record(rec, rec_id: nil)
    #$logger.info("adding record")
    if rec.is_a?(Hash)
      @records << [self.flatten_hash(rec), rec_id]
    else
      @records << [rec, rec_id]
    end
    if @records.length > @autocommit_threshold
      self.commit
    end
  end #def add_record
  
  def commit
    actions = []
    $logger.info("Committing to index #{INDEXNAME}...")
    @records.each do |ent|
        rec = ent[0]
        rec_id = ent[1]
        data = {
            _index: INDEXNAME,
            _type: TYPENAME,
            data: rec
        }
        data['_id'] = rec_id if(rec_id != nil)
      actions << { index: data}
    end
    @client.bulk(body: actions)
    @records = []
  end #def commit
  
end #class ElasticIndexer

def podcast_details(path)
  parts=path.split(/\//)
 
  if parts.length<8
    return nil
  end
  
  type=nil
  m=/\.([^\.]+)/.match(path)
  if m
    type=m.captures[0]
  end
  
  [3, 5, 8].each do |n|
    #if any of these key components are numbers only, then it's not a podcast.
    return nil if /^[\s\d]+$/.match(parts[n])
  end
  
  begin
    {'section'=>parts[3],'series'=>parts[5],'filename'=>parts[8],'type'=>type}
  rescue StandardError=>e
    $logger.warn(e)
  end

end

def video_details(target_url)
    base = File.basename(target_url)
    #remove any url args
    base.sub!(/[?;].*$/,'')
    
    rtn = {}
    
    parts = /^(.*)\.([^\.]+)/.match(base)
    if(parts)
        #parts[1] is filename, parts[2] is extension
        filename_tokens = parts[1].split(/_/)
        if(parts[2] == "m3u8") #these have extra tokens
            rtn['encoding_type']='HLS'
            if(filename_tokens[-1].match(/\d+[mMkK]{0,1}/)) #this has a bitrate specifier so is a submanifest
                $logger.debug("Filename final token '#{filename_tokens[-1]}' matches (digit[m,k]*) so this is a submanifest")
               rtn['manifest_type'] = 'submanifest'
            else
            $logger.debug("Filename final token '#{filename_tokens[-1]}' does not match (digit[m,k]*) so this is a main manifest")
               rtn['manifest_type'] = 'master'
            end
        else
            rtn['encoding_type'] = filename_tokens[-1]
        end
     end #if(parts)
    return nil if(rtn.count==0)
    return rtn
end #def video_details

def parse_string(str,extra_data: {}, indexer: nil)
  #grok = Grok.new
  #grok.add_patterns_from_file('patterns/base')
  
  #grok.add_pattern("basic test",'%{TIMESTAMP_ISO8601:time} %{NOTSPACE:pop} %{WORD:destination}\[%{NUMBER:pid}\]: %{IP:client} %{QUOTEDSTRING:unknown1} %{QUOTEDSTRING:unknown2} %{DAY:day}, %{NUMBER:date} %{WORD:month} %{NUMBER:year} %{NUMBER:hour}:%{NUMBER:min}:%{NUMBER:sec} %{WORD:timezone} %{WORD:verb} %{URIPATH:target}')
  #grok.compile('%{TIMESTAMP_ISO8601:time} %{NOTSPACE:pop} %{WORD:destination}\[%{NUMBER:pid}\]: %{IP:client} %{QUOTEDSTRING:unknown1} %{QUOTEDSTRING:unknown2} %{DAY:day}, %{NUMBER:date} %{WORD:month} %{NUMBER:year} %{NUMBER:hour}:%{NUMBER:min}:%{NUMBER:sec} %{WORD:timezone} %{WORD:verb} %{URIPATH:target}')
  #grok.compile('%{WORD:verb} %{URIPATH:target}')
  #puts grok.expanded_pattern()
  
  begin
    g = GeoIP.new('GeoIP.dat')
  rescue StandardError=>e
    puts e.message
    puts e.backtrace
    g = nil
  end
  
  begin
    c = GeoIP.new('GeoLiteCity.dat')
  rescue StandardError=>e
    puts e.message
    puts e.backtrace
    c = nil
  end
  
  if str.is_a?(StringIO)
    str=str.string
  end
  
  matcher = Regexp.new('(?<datestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) (?<pop>[\w\d\-]+) (?<destination>[\w\d]+)\[(?<pid>\d+)\]: (?<client>\d+\.\d+\.\d+\.\d+) \".*\" \".*\" .* (?<verb>\w+) (?<target>[^\s]+) (?<response>\d+)$')
  str.encode('utf-8').split(/\n/).each {|line|
    #puts line
    line.chomp!
    match=matcher.match(line)
    if match==nil
      $logger.warn("Unable to parse line: #{line}")
      next
    end
    rtn=Hash[match.names.zip(match.captures)]
    rtn['line']=line
    if rtn['datestamp']
      rtn['datestamp']=DateTime.parse(rtn['datestamp'])
      rtn['@timestamp']=rtn['datestamp']
    end
    
    d=/\.([^\.]+)$/.match(rtn['target'])
    if d
      rtn['file_extension']=d[1]
    end
    
    target_details=podcast_details(rtn['target'])
    if target_details
      target_details.each {|k,v|
       rtn[k]=v  
      }
    end
    
    target_details=video_details(rtn['target'])
    if target_details
        target_details.each {|k,v|
            rtn[k]=v
        }
    end
    
    if g
      countrydata=g.country(rtn['client'])
      rtn['country_code']=countrydata[:country_code]
      rtn['country_code2']=countrydata[:country_code2]
      rtn['country_code3']=countrydata[:country_code3]
      rtn['country_name']=countrydata[:country_name]
      rtn['continent_code']=countrydata[:continent_code]
    end
    
    if c
      citydata=c.city(rtn['client'])
      if citydata
          rtn['city_name']=citydata[:city_name]
          rtn['postal_code']=citydata[:postal_code]
          #rtn['latitude']=citydata[:latitude]
          #rtn['longitude']=citydata[:longitude]
          rtn['location'] = {
            'lat' => citydata[:latitude],
            'lon' => citydata[:longitude]
          }
          rtn['dma_code']=citydata[:dma_code]
          rtn['area_code']=citydata[:area_code]
          rtn['timezone']=citydata[:timezone]
          rtn['region_name']=citydata[:real_region_name]
          #ap citydata
      end
    end
    
    rtn.merge!(extra_data)
    #raise StandardError, "Testing"
    doc_id = Base64.encode64(rtn['line'])
    indexer.add_record(rtn, rec_id: doc_id)
  }
  indexer.commit
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

#START MAIN
$opts = Trollop::options do
  opt :elasticsearch, "Location of elasticsearch cluster to communicate with. Specify multiple hosts separated by commas.", :type=>:string, :default=>"localhost"
  opt :queueurl, "URL of the Amazon SQS queue to listen to", :type=>:string
  opt :region, "AWS region to operate in", :type=>:string, :default=>"eu-west-1"
  opt :testfile, "Test run on the provided file", :type=>:string
  opt :reindex_bucket, "Re-index from the specified bucket", :type=>:string
end

if($opts.testfile)
    $logger = Logger.new(STDOUT)
    $logger.level = Logger.DEBUG
    $logger.info("Starting test on #{$opts.testfile}")
    File.open($opts.testfile) do |f|
        parse_string(f.read,extra_data: {'domain' => '(test)'}, indexer: ElasticIndexer.new(client: ets, autocommit: 500))
    end #File.open
    exit(0)
end

$logger=Logger.new(LOGFILE)
#$logger=Logger.new(STDOUT)
$logger.level=Logger::DEBUG

ets = Elasticsearch::Client.new(hosts: $opts.elasticsearch.split(/,\s*/),log: true)
ets.cluster.health

if not ets.indices.exists?(index: INDEXNAME)
    ets.indices.create(index: INDEXNAME,body: {
                       settings: {
                       analysis: {
                       analyzer: {
                       path: {
                       tokenizer: "path_hierarchy",
                       type: "custom",
                       }
                       }
                       }
                       },
                       mappings: {
                       log: {
                       properties: {
                       pop: {type: "string", index: "not_analyzed"},
                       city_name: {type: "string", index: "not_analyzed"},
                       continent_code: {type: "string", index: "not_analyzed"},
                       country_name: {type: "string", index: "not_analyzed"},
                       client: {type: "ip"},
                       location: {type: "geo_point"},
                       filename: {type: "string", index: "not_analyzed"},
                       postal_code: {type: "string", index: "not_analyzed"},
                       region_name: {type: "string", index: "not_analyzed"},
                       section: {type: "string", index: "not_analyzed"},
                       series: {type: "string", index: "not_analyzed"},
                       target: {type: "string", analyzer: "path"},
                       file_extension: {type: "string", index: "not_analyzed"}
                       }
                       }
                       }
                       })
end

if($opts.reindex_bucket)
    $logger.info("Attempting re-index from #{$opts.reindex_bucket}")
    s3 = Aws::S3::Client.new(region: $opts.region)
    b = Aws::S3::Bucket.new($opts.reindex_bucket, client: s3)
    
    b.objects.each do |obj|
        content = download_from_s3(bucket: $opts.reindex_bucket,key: obj.key)
        domain = obj.key.split(/\//)[0]
        if domain == "" #leading / confuses things somewhat
            domain = obj.key.split(/\//)[1]
        end
        
        #raise StandardError, "Testing"
        $logger.info("Parsing...")
        parse_string(content,extra_data: {'domain' => domain}, indexer: ElasticIndexer.new(client: ets,autocommit: 500))
        $logger.info("Done.")
    end #b.objects.each
    exit(0)
end

if $opts[:queueurl]==nil
  $logger.error("You need to specify a queue to listen to using --queueurl")
  exit(1)
end

c=Aws::SQS::Client.new(region: $opts.region)

Aws::SQS::QueuePoller.new($opts[:queueurl], {:client=>c}).poll do |msg|
  begin
    #puts msg
    $logger.debug("Got message #{msg.body}")
    data=JSON.parse(msg.body)
    if not data['Event']
      data['Event']="new"
    end
    case data['Event']
    when 'new'
      $logger.info("Downloading #{data['Key']} from #{data['Bucket']}")
      content = download_from_s3(bucket: data['Bucket'],key: data['Key'])
      domain = data['Key'].split(/\//)[0]
      if domain == "" #leading / confuses things somewhat
        domain = data['Key'].split(/\//)[1]
      end
      
      #raise StandardError, "Testing"
      $logger.info("Parsing...")
      parse_string(content,extra_data: {'domain' => domain}, indexer: ElasticIndexer.new(client: ets,autocommit: 500))
      $logger.info("Done.")
    else
      $logger.error("Unknown event type #{data['Event']}")
    end
  rescue StandardError=>e
    $logger.error(e)
  end
end
#test elasticsearch


