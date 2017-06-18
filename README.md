# VKitM - Virtual Kafka in the Middle [![Build Status](https://travis-ci.org/mmolimar/vkitm.svg?branch=master)](https://travis-ci.org/mmolimar/vkitm)[![Coverage Status](https://coveralls.io/repos/github/mmolimar/vkitm/badge.svg?branch=master)](https://coveralls.io/github/mmolimar/vkitm?branch=master)

VKitM is a [Apache Kafka](https://kafka.apache.org/) proxy which uses the native Kafka protocol.

Its name is due to how it has been designed: the application acts as an intermediary between the Kafka client 
and the Kafka cluster, modifying the requests and responses from/to the client. 
Actually, the client thinks that the connection is to a Kafka cluster (by now, with one node) and 
doesn't know what's behind (an entirely Kafka cluster).

Right now, the implementation includes a subset of the [API Keys](https://kafka.apache.org/protocol#protocol_api_keys)
but can produce and consume messages.

## How it works

1. Kafka client connects to a single Kafka broker. Actually, it's connecting to a VKitM.
2. VKitM modifies the request, depending of the API Key.
3. The modified request is sent to the actual Kafka cluster, if applicable.
3. VKitM receives the response from the Kafka brokers.
4. Based on the kind of response it has to send to the client, VKitM changes data related with brokers, 
   partition leaders and other metadata in the final response.
5. The client receives the response thinking that there is just one node in the cluster and continues.

**DISCLAIMER:** This application has been implemented in order to provide another option when a proxy 
is required. If you use VKitM with a different purpose from the one has been designed it's under your own 
responsibility.

## Motivation

There are multiple Kafka proxies out there but most of them are HTTP REST based. A well-known 
REST Proxy is [this one](https://github.com/confluentinc/kafka-rest).

However, most of them lack of some of the following features I'd like to provide in VKitM:

- Use the native Kafka protocol directly, **improving the performance**.
- Be able to **modify the messages from the clients on the fly** when we cannot do it in the client itself
  (due to business cases or whatever) including **[interceptors](https://cwiki.apache.org/confluence/display/KAFKA/KIP-42%3A+Add+Producer+and+Consumer+Interceptors)**.
- Define **custom ACLs** to complement those that already exist (or maybe not) in the Kafka Cluster.
- Change the **kind of protocol** in your clients (secured or not). I.e.: using TLS connection from the clients to VKitM
  and from VKitM to Kafka brokers in plaintext (or vice versa) or even change the certificates they use.

## Getting started

### Building source jar ###
    gradle clean build

### Run VKitM ###
    java -jar build/libs/vkitm-{VERSION}.jar src/main/resources/application.conf

**NOTE**: modify ``src/main/resources/application.conf`` file, if applies, to your custom configuration.

## Docker

You can run VKitM inside a Docker container. Use the image hosted in [Docker Hub](https://hub.docker.com/r/mmolimar/vkitm/) 
or just build your own one.

### Building the image ###
    docker build -t mmolimar/vkitm --build-arg VERSION=<VERSION> .

### Run VKitM inside a container ###
    docker run -p <HOST_PORT>:<CONTAINER_PORT> -v /path/to/conf/directory:/vkitm/conf mmolimar/vkitm

**NOTE**: path ``/path/to/conf/directory`` must contain a file named ``application.conf`` with the VKitM configuration.

## Future work

- Implement most of the Kafka protocol API Keys.
- Develop a VKitM cluster, avoiding SPOF (Single Point Of Failure) which now exists.

## Contribute

- Source Code: https://github.com/mmolimar/vkitm
- Issue Tracker: https://github.com/mmolimar/vkitm/issues

## License

Released under the Apache License, version 2.0.