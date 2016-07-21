#!/bin/bash
# Copyright (C) 2016 Hewlett-Packard Development Company, L.P.
# All Rights Reserved.
#

OVSTESTDIR="/ovstest"
DOCKERIMAGE="openswitch/ubuntudev"
TESTCONTAINER="test"
ERRORS=0

echo "Loading docker"
# If docker image is not available, it will be downloaded from openswitch docker hub.
if [ $(docker images | grep -ic $DOCKERIMAGE) -le 1 ]; then
    docker pull $DOCKERIMAGE
    if [ $(echo $?) -eq 1 ]; then
        echo "Docker image not found"
        exit 1;
    fi
    until [ $(docker images | grep -ic $DOCKERIMAGE) -eq 1 ]; do
       sleep 0.1;
    done;
fi
docker run --rm -v $(pwd):${OVSTESTDIR} --name ${TESTCONTAINER} ${DOCKERIMAGE} &
sleep 2
echo "Waiting docker to be ready"
until [ "`/usr/bin/docker inspect -f {{.State.Running}} ${TESTCONTAINER}`"=="true" ]; do
    sleep 0.1;
done;
echo "Configuring options in docker"
# Need to create an user in the docker with the same attributes as in local host
# to avoid creating files with root permits
docker exec  ${TESTCONTAINER} groupadd -g $(id -g ${USER}) $(stat -c "%G" .)
docker exec  ${TESTCONTAINER} useradd  -u $(id -u ${USER}) ${USER}
echo "Running tests in docker"
docker exec  ${TESTCONTAINER} /bin/sh -c "cd ${OVSTESTDIR} && sudo -u ${USER} ./ovs-test.sh"
docker stop test
