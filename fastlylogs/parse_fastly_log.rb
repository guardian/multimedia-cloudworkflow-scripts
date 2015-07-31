#!/usr/bin/env ruby

#gem install jls-grok ffi
#require 'grok'
require 'awesome_print'
require 'date'
require 'geoip'
require 'logger'
require 'elasticsearch'

INDEXNAME='fastlylogs'
TYPENAME="log"

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
    print "flatten_hash got:"
    ap(h)
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
    print "flatten_hash returned:"
    ap(h)
    return h
  end #def flatten.hash
  
  def add_record(rec)
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
  
  begin
    {'section'=>parts[3],'series'=>parts[5],'filename'=>parts[8],'type'=>type}
  rescue StandardError=>e
    $logger.warn(e)
  end

end

def parse_string(str,indexer: nil)
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
  
  matcher = Regexp.new('(?<datestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) (?<pop>[\w\d\-]+) (?<destination>[\w\d]+)\[(?<pid>\d+)\]: (?<client>\d+\.\d+\.\d+\.\d+) \".*\" \".*\" .* (?<verb>\w+) (?<target>[A-Za-z0-9$.+!*\'(){},~:;=@#%_\-\/?]+) (?<response>\d+)$')
  str.split(/\n/).each {|line|
    puts line
    line.chomp!
    match=matcher.match(line)
    rtn=Hash[match.names.zip(match.captures)]
    rtn['line']=line
    if rtn['datestamp']
      rtn['datestamp']=DateTime.parse(rtn['datestamp'])
      rtn['@timestamp']=rtn['datestamp']
    end
    target_details=podcast_details(rtn['target'])
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
    
    #raise StandardError, "Testing"
    indexer.add_record(rtn)
  }
  indexer.commit
end

#START MAIN
#parse_file(ARGV[0])

ets = Elasticsearch::Client.new(hosts: ['dc1-workflow-01.dc1.gnm.int'],log: true)
ets.cluster.health

if ets.indices.exists?(index: INDEXNAME)
  ets.indices.delete(index:INDEXNAME)
end

if not ets.indices.exists?(index: INDEXNAME)
  ets.indices.create(index: INDEXNAME, body: {
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
                          }
                        }
                      }
  })
end

File.open(ARGV[0]) do |f|
  parse_string(f.read(), indexer: ElasticIndexer.new(client:ets, autocommit:500))
end

#test elasticsearch


