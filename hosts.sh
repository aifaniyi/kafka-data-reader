#!/bin/bash

filename=/etc/hosts
#
# for all running docker containers
#
for service in `docker ps -q`; do
   #
   # Extract the servicename and ipaddress
   #
   servicename=`docker inspect --format '{{ .Name }}' $service `
   ipaddress=`docker inspect --format '{{ .NetworkSettings.Networks.kafka_data_reader_default.IPAddress }}' $service`
  
   #
   # if there is a service name and ipaddress
   #
   if [ ! -z $ipaddress ] &&  [ ! -z $servicename ] ;
   then
        # get rid of the first character - this is '/'  
        servicename=${servicename:1}

        # 
        if grep -Fq "$servicename #DOCKER_IP_ADDRESS" $filename
        then
            # if entry already exists; then do nothing
            :
        else
            echo "$servicename #DOCKER_IP_ADDRESS" >> $filename
        fi

        cp /etc/hosts /etc/hosts.bak
        sed -i "/$servicename #DOCKER_IP_ADDRESS/c\\$ipaddress $servicename #DOCKER_IP_ADDRESS" /etc/hosts
   fi
done