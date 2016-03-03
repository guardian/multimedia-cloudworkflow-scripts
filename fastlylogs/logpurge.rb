#!/usr/bin/env ruby

require 'logger'
require 'elasticsearch'
require 'trollop'
require 'awesome_print'

INDEXNAME="fastlylogs"
INDEXTYPE="log"
COMMIT_INTERVAL=2000

#START MAIN
opts = Trollop::options do
  opt :elasticsearch, "Elasticsearch hosts", :type=>:string
  opt :repository, "Elasticsearch snapshot repository", :type=>:string, :default=>"s3_snapshot_repo"
end

logger = Logger.new(STDOUT)

logger.info("Connecting to Elasticsearch on #{opts.elasticsearch}")
es = Elasticsearch::Client.new(:hosts=>opts.elasticsearch)


timestr = DateTime.now.strftime("%Y%m%d_%H%M%S")
snap_name = "#{INDEXNAME}_automated_#{timestr}"
logger.info("Starting backup to #{opts.repository} : #{snap_name}")

es.snapshot.create(repository: opts.repository, snapshot: snap_name, wait_for_completion: true)
logger.info("Completed")

logger.info("Starting autopurge...")
resultset = es.search(index: INDEXNAME, search_type: "scan", scroll: "5m", body: {
  'query'=> {
    'filtered'=> {
      'filter'=> {
        'range'=> {
          'datestamp'=> {
            "lte"=>"now-15d"
          }
        }
      }
    }
  }
})


ap resultset['_scroll_id']
bulklist = []

while resultset = es.scroll(scroll_id: resultset['_scroll_id'], scroll: "5m") and not resultset['hits']['hits'].empty? do
  resultset['hits']['hits'].each {|entry|
    bulklist << { delete: {_index: INDEXNAME, _type: INDEXTYPE, _id: entry['_id']}}
  }
  if bulklist.length>COMMIT_INTERVAL
    logger.info("Purging #{COMMIT_INTERVAL} records...")
    es.bulk(body: bulklist)
    bulklist = []
  end
  
  #ap bulklist
end
logger.info("Done")
