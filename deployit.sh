#!/usr/bin/env bash

#from http://stackoverflow.com/questions/3915040/bash-fish-command-to-print-absolute-path-to-a-file
function abspath() {
    # generate absolute path from relative path
    # $1     : relative filename
    # return : absolute path
    if [ -d "$1" ]; then
        # dir
        (cd "$1"; pwd)
    elif [ -f "$1" ]; then
        # file
        if [[ $1 == */* ]]; then
            echo "$(cd "${1%/*}"; pwd)/${1##*/}"
        else
            echo "$(pwd)/$1"
        fi
    fi
}

DEPLOYPATH="$1/cloudworkflowscripts"
BASEPATH=$(abspath "${BASH_SOURCE%/*}")

#echo ${BASH_SOURCE}
if [ ${DEPLOYPATH} == "" ]; then
    echo Usage: ./deployit.sh s3://path/to/deploy
    exit 1
fi

echo Running in ${BASEPATH}

echo -----------------------------------
echo Building gems...
echo -----------------------------------
cd ${BASEPATH}/endpointerrors
gem build endpointerrors.gemspec
cd ${BASEPATH}

echo -----------------------------------
echo Building new tar bundle...
echo -----------------------------------
tar cv --exclude=".git/" --exclude=".vagrant/" --exclude=".*/" * | bzip2 > /tmp/cloudworkflowscripts.tar.bz2
if [ "$?" != "0" ]; then
    echo tar bundle failed to build :\(
    exit 1
fi

echo -----------------------------------
echo Moving old tar bundle on S3...
echo -----------------------------------
aws s3 mv "${DEPLOYPATH}/cloudworkflowscripts.tar.bz2"  "${DEPLOYPATH}/cloudworkflowscripts_$(date +%Y%m%d_%H%M%S).tar.bz2"
if [ "$?" != "0" ]; then
    echo aws command failed :\(
    exit 1
fi

echo -----------------------------------
echo Deploying new bundle to S3...
echo -----------------------------------
aws s3 cp  /tmp/cloudworkflowscripts.tar.bz2 "${DEPLOYPATH}/cloudworkflowscripts.tar.bz2"
if [ "$?" != "0" ]; then
    echo aws command failed :\(
    exit 1
fi

rm -f /tmp/cloudworkflowscripts.tar.bz2

echo All done!
