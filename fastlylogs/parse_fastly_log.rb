#!/usr/bin/env ruby

#gem install jls-grok ffi
#require 'grok'
require 'awesome_print'
require 'date'
require 'geoip'
require 'logger'

$logger=Logger.new(STDOUT)
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

def parse_string(str)
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
  
  matcher = Regexp.new('(?<datestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) (?<pop>[\w\d\-]+) (?<destination>[\w\d]+)\[(?<pid>\d+)\]: (?<client>\d+\.\d+\.\d+\.\d+) \".*\" \".*\" .* (?<verb>\w+) (?<target>[A-Za-z0-9$.+!*\'(){},~:;=@#%_\-\/]+) (?<response>\d+)$')
  str.split(/\n/).each {|line|
    puts line
    line.chomp!
    match=matcher.match(line)
    rtn=Hash[match.names.zip(match.captures)]
    if rtn['datestamp']
      rtn['datestamp']=DateTime.parse(rtn['datestamp'])
    end
    rtn['target_details']=podcast_details(rtn['target'])
    if g
      rtn['client_country']=g.country(rtn['client'])
    end
    
    ap rtn
  }
end

#START MAIN
#parse_file(ARGV[0])
File.open(ARGV[0]) do |f|
  parse_string(f.read())
end