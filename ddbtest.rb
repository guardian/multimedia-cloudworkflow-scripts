#!/usr/bin/env ruby

require 'rubygems'
require 'aws-sdk'
#require 'uuid'

ddb=AWS::DynamoDB.new(:region=>'eu-west-1')

#ddb.tables.each do |table|
#	puts "Found table #{table.name}"
#end

table=ddb.tables['workflowmaster-cds-routes']
table.hash_key = [:routename,:string]

table.items.each do |item|
	puts item.hash_value
	item.attributes.each_key do |key|
		puts "\t#{key} => #{item.attributes[key]}\n";
	end
end

