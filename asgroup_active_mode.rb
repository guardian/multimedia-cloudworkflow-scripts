#!/usr/bin/env ruby

require 'aws-sdk';

def usage
	puts "Put the given autoscaling group into 'active mode', i.e. set the minimum and desired capacity to one."
	exit 1
end

#START MAIN
$asref=AWS::AutoScaling.new(:region=>'eu-west-1')

if(ARGV.count!=1) then
	usage
	exit 1
end

puts "Autoscaling groups:"
$asref.groups.each{ |group|
	puts "\t#{group.name}"
}
groupname=ARGV[0]
asgroup=$asref.groups[groupname]
unless(asgroup.exists?) then
	puts "The autoscaling group #{groupname} does not exist."
	exit 2
end

if(asgroup.min_size>0) then
	puts "Minimum size for #{groupname} is already at #{asgroup.min_size} so I won't over-ride this."
else
	asgroup.update(:min_size=>1)
end

if(asgroup.desired_capacity>0) then
	puts "Desired capacity for #{groupname} is already at #{asgroup.desired_capacity} so I won't over-ride this."
else
	asgroup.set_desired_capacity(0,:honor_cooldown=>true)
end

puts "Updated scaling group #{groupname}:"
puts "\tmin_size: #{asgroup.min_size}"
puts "\tmax_size: #{asgroup.max_size}"
puts "\tdesired size: #{asgroup.desired_capacity}"
puts "\tLaunch configuration: #{asgroup.launch_configuration_name}"
puts "\tDefault cooldown: #{asgroup.default_cooldown}"
puts "\tHealth check grace period: #{asgroup.health_check_grace_period}"
puts "\tLoad balancers:"
asgroup.load_balancer_names.each{ |name|
	puts "\t\t#{name}"
}
puts "\tInstances:"
asgroup.auto_scaling_instances.each { |instance|
	puts "\t\tInstance ID: #{instance.instance_id}"
	puts "\t\tLifecycle state: #{instance.lifecycle_state}"
	puts "\t\tHealth status: #{instance.health_status}"
}

