#!/usr/bin/env ruby

require 'aws-sdk-v1';

def usage
	puts "Put the given autoscaling group into 'weekend mode', i.e. set the minimum capacity to zero.  Desired capacity is left as-is in case a job is still in progress"
	puts "To undo this action, either run asgroup_active_mode to set the capacity back up to 1 or over-ride it in the EC2 Management Console"
	exit 1
end

#START MAIN
attempts=100
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

n=0
begin
asgroup.update(:min_size=>0)
#asgroup.set_desired_capacity(0,:honor_cooldown=>true)

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

rescue Exception =>e
	n+=1
	puts "Unable to perform autoscaling action on attempt #{n} of #{attempts}: #{e.message}. Will retry in 2 minutes"
	sleep(120)
	if(n<attempts) then
		retry
	end
	exit 255
end

