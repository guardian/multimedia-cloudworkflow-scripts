#!/bin/bash

gem2.0 install endpointerrors-1.0.gem
cp upstart/trap_endpoint_errors.conf /etc/init
cp logrotate/trap_endpoint_errors.conf /etc/logrotate.d
initctl start trap_endpoint_errors
