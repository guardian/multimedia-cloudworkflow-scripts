#!/usr/bin/env ruby

require 'aws-sdk-resources'
require 'awesome_print'
require 'elasticsearch'
require 'logger'
require 'trollop'
require 'date'

LOGFILE = "/var/log/s3_elastic_index.log"
INDEXNAME = "s3_index"
TYPENAME = "s3_entry"

class ElasticIndexer
  def initialize(client: nil,autocommit: 0,logger: Logger.new(LOGFILE), loglevel: Logger::INFO)
    @records = []
    @autocommit_threshold = autocommit
    
    @logger=logger
    @logger.formatter = proc do |severity, datetime, progname, msg|
      "#{datetime}: #{severity} [ElasticIndexer] thread: #{Thread.current.object_id}: #{msg}\n"
    end
    
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
    @logger.info("adding record")
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
    @logger.info("Committing to index #{INDEXNAME}...")
    
    @records.each do |rec|
      if rec['_type']
        t = rec['_type']
        rec.delete('_type')
      else
        t = TYPENAME
      end
      actions << { index: {
        _index: INDEXNAME,
        _type: t,
        data: rec
      }}
    end
    @client.bulk(body: actions)
    @records = []
  end #def commit
  
end #class ElasticIndexer

#this is called in a subthread to process a bucket's worth of stuff
def process_bucket(bucketname,options: {},logger: Logger.new(LOGFILE))
  
  client = Elasticsearch::Client.new(host: options.elasticsearch, log: false)
  indexer = ElasticIndexer.new(client: client, autocommit: 200,logger: logger)
  
  logger.info("Scanning #{bucketname}")
  Aws::S3::Bucket.new(bucketname).objects.each {|objectsummary|
    logger.info("got key #{objectsummary.key}")
    ct_major = ""
    ct_minor = ""
    parts = /^(.*)\/(.*)$/.match(objectsummary.object.content_type)
    if parts
      ct_major = parts[1]
      ct_minor = parts[2]
    else
      logger.warn("Unable to parse content type '#{objectsummary.object.content_type}'")
    end
    ap objectsummary.object.metadata
    
    indexer.add_record({
      bucket: bucketname,
      etag: objectsummary.etag,
      path: objectsummary.key,
      last_modified: Date.parse(objectsummary.last_modified.to_s).iso8601,
      owner: {
        display_name: objectsummary.owner.display_name,
        id: objectsummary.owner.id
      },
      size: objectsummary.size,
      content_type: {
        major: ct_major,
        minor: ct_minor,
        raw: objectsummary.object.content_type
      },
      storage_class: objectsummary.storage_class,
      content_encoding: objectsummary.object.content_encoding,
      content_disposition: objectsummary.object.content_disposition,
      extra_data: objectsummary.object.metadata
    })
  }
rescue Aws::S3::Errors::PermanentRedirect=>e
  logger.error(e.message)
end

#START MAIN
logger = Logger.new(STDOUT)
logger.level = Logger::INFO

opts = Trollop::options do
  opt :elasticsearch, "host:port for elasticsearch cluster", :default=>"localhost:9200"
  opt :threads, "number of buckets to scan in parallel", :default=>5
end

client = Elasticsearch::Client.new(host: opts.elasticsearch, log: false)

if not client.indices.exists?(index: INDEXNAME)
  logger.info("Index #{INDEXNAME} does not exist, creating...")
  client.indices.create(index: INDEXNAME,
                        body: {
                        settings: {  
                         analysis: {
                          tokenizer: {
                            path_tokenizer: {
                              type: "PathHierarchy"
                            },
                            mime_tokenizer: {
                              type: "pattern",
                              pattern: "/"
                            }
                          },
                          analyzer: {
                            path_analyzer: {
                              tokenizer: "path_tokenizer"
                            },
                            mime_analyzer: {
                              tokenizer: "mime_tokenizer"
                            }
                          }
                         }
                        },
                        mappings: {
                          "#{TYPENAME}"=>{
                            properties: {
                              bucket: {
                                type: "string",
                                index: "not_analyzed"
                              },
                              etag: {
                                type: "string",
                                index: "not_analyzed"
                              },
                              path: {
                                type: "string",
                                analyzer: "path_analyzer"
                              },
                              last_modified: {
                                type: "date"
                              },
                              owner: {
                                type: "object",
                                properties: {
                                  display_name: {
                                    type: "string",
                                    index: "not_analyzed"
                                  },
                                  id: {
                                    type: "string",
                                    index: "not_analyzed"
                                  }
                                }
                                
                              },
                              size: {
                                type: "long"
                              },
                              storage_class: {
                                type: "string",
                                index: "not_analyzed"
                              },
                              content_type: {
                                type: "object",
                                properties: {
                                  major: {
                                    type: "string",
                                    index: "not_analyzed"
                                  },
                                  minor: {
                                    type: "string",
                                    index: "not_analyzed"
                                  },
                                  raw: {
                                    type: "string",
                                    analyzer: "mime_analyzer"
                                  }
                                }
                              },
                              content_encoding: {
                                type: "string",
                                index: "not_analyzed"
                              },
                              content_disposition: {
                                type: "string",
                                index: "not_analyzed"
                              },
                              extra_data: {
                                type: "object",
                                properties: {
                                  "s3cmd-attrs"=>{
                                    type: "string",
                                    analyzer: "mime_analyzer"
                                  }
                                }
                              }
                            }
                          }
                        }
                        }
  )
end

threads = []
queue = Queue.new()
opts.threads.times do
  threads << Thread.new {
    while true do
      bucket = queue.deq
      if bucket==nil
        break
      end
      process_bucket(bucket.name, options: opts, logger: logger)
    end
  }
end

s3_client = Aws::S3::Client.new
s3_client.list_buckets.buckets.each {|bucketname|
  queue << bucketname 
}

opts.threads.times do
  queue << nil
end

logger.info("Waiting for threads to terminate...")
threads.each do |t|
  t.join()
end

#process_bucket("gnm-multimedia-loosearchive", options: opts, logger: logger)
