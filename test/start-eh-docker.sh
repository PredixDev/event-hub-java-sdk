#!/usr/bin/env bash
unset default_proxy

while getopts 'c' flag; do
    case "${flag}" in
        c)
            echo "Stopping and removing old containers"
            # stop any existing containers
            docker-compose stop zookeeper
            docker-compose stop kafka
            docker-compose stop eventhub
            docker-compose stop eventhubsdktest

            docker-compose rm -f zookeeper
            docker-compose rm -f kafka
            docker-compose rm -f eventhub
            docker-compose rm -f eventhubsdktest

    ;;
  esac
done
docker-compose build
docker-compose up -d zookeeper
docker-compose up -d kafka
docker-compose up -d eventhub
docker-compose up eventhubsdktest
status=`docker inspect test_eventhubsdktest_1 --format='{{.State.ExitCode}}'`

if [ ${status} -ne 0 ]
then
    echo "Finished: Running Tests"
    echo "Finished: Failed"
    exit 1
else
    echo "Finished: Running Tests"
    echo "Finished: Success"
    exit 0
fi



#docker run --rm --name event-hub-java-sdk --network="eventhub_test_network" -v $(pwd):/opt/maven -w /opt/maven  maven:latest chmod +x test/docker-test-environment-variables.sh && ./test/docker-test-environment-variables.sh &&  mvn clean install'
