#!/bin/bash

set -x

if [ "x$BRICK_ROOT" != "x" ]
then
    pkill gluster
    rm -rf /var/lib/glusterd
    rm -rf /var/log/glusterfs
    glusterd -LDEBUG
    rm -rf $BRICK_ROOT/
    mkdir -p $(dirname $BRICK_ROOT/gv1/brick1/brick)
    dub test
fi
