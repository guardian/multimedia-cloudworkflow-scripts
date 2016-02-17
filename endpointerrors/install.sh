#!/bin/bash

gem2.0 install endpointerrors-1.0.gem
cp upstart/trap_endpoint_errors.conf /etc/init
initctl start trap_endpoint_errors
