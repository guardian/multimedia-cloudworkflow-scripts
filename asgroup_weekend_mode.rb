#!/usr/bin/env ruby

require 'aws-sdk';

def usage
	puts "Put the given autoscaling group into 'weekend mode', i.e. set the minimum and desired capacity to zero."
	puts "To undo this action, either run asgroup_active_mode to set the capacity back up to 1 or over-ride it in the EC2 Management Console"
	exit 1
end

#START MAIN
$asref=AWS::AutoScaling.new()

if(ARGV.count!=1){
	usage
	exit 1
}

groupname=ARGV[0]
asgroup=$asref.groups[groupname]
unless(asgroup.exists?){
	puts "The autoscaling group #{groupname} does not exist."
	exit 2
}

asgroup.update(:min_size=>0)
asgroup.set_desired_capacity(0,:honor_cooldown=>true)

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

