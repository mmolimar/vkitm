# VKitM - Virtual Kafka in the Middle
VKitM is a native [Apache Kafka](https://kafka.apache.org/) proxy using the native Kafka protocol.

Its name is due to how has been designed: the application acts as an intermediary between the Kafka client 
and the Kafka cluster, modifying the requests and responses from/to the client. 
Actually, the client thinks that the connection is to a Kafka cluster (by now, with one node) and 
doesn't now what's behind (an entirely Kafka cluster).

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
responsability.

## Motivation

There are multiple Kafka proxies out there but most of them are HTTP REST based. A well-known 
REST Proxy is [this](https://github.com/confluentinc/kafka-rest).

However, most of them lack of some of the following features I'd like to provide in VKitM:

- Use the native Kafka protocol directly, improving the performance.
- To be able to modify the messages from the clients on the fly when we cannot do it in the client itself
  (due to business cases or whatever) including [interceptors](https://cwiki.apache.org/confluence/display/KAFKA/KIP-42%3A+Add+Producer+and+Consumer+Interceptors).
- Define custom ACLs to complement those that already exist in the Kafka Cluster.

## Getting started

### Building source jar ###
    gradle clean build

**NOTE**: modify ``src/main/resources/application.conf`` file, if applies, to your custom configuration.

### Run VKitM ###
    java -jar build/libs/vkitm-{VERSION}.jar

## Future work

- Implement most of the Kafka protocol API Keys.
- Develop a VKitM cluster, avoiding SPOF (Single Point Of Failure) which now exists.
- 'Dockerizer' the app.

## Contribute

- Source Code: https://github.com/mmolimar/vkitm
- Issue Tracker: https://github.com/mmolimar/vkitm/issues

## License

Released under the Apache License, version 2.0.