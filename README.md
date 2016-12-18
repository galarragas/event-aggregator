# event-aggregator
Tool To Aggregate Events from Kafka Topics into Elasticsearch

## Event Aggregator

The event aggregator is a simple infrastructure component designed to consume events written into __Kafka__ in __AVRO__ format and write them into __Elasticsearch__.
Event schemas are maintained into a __Schema Registry__. The one currently supported is the [Confluent Schema Registry](http://docs.confluent.io/2.0.0/schema-registry/docs/index.html)

The tool is essentially a rewriting for open source purposed of a similar tool I wrote and extensively used when working on a microservice event driven system.
It proven very useful to me so I think it would have been good to share with the community.

Feel free to use it and extend it.

## Purpose

One of the major infrastructural requirements when implementing a distributed system, and mostly when working on a microservice architecutre, is __log aggregation__. 
Developers and administrators need to be able to have a centralised view of how a user requrest propagates through the system and to trace when an error occurs and how 
it propagates.

An event based system imposes that the main interactions across the different services are based on __events__ published into an __event bus__. 
Different (micro)services are then able to subscribe to different __event topics__ and react to the different events.

In such an architecture is therefore highly beneficial to be able to browse, search and inspect events in the same place where the services logs are.
This will allow to completely track the overall system reaction to every event. 

If you think about a REST based microservice system, the event aggregation plays a similar role of having the HTTP trafic in your log aggregator.

Well designed events would include tracing facilities such as [Correlation IDs](http://www.enterpriseintegrationpatterns.com/patterns/messaging/CorrelationIdentifier.html) 
that is used to track the whole flow of events originated by the same source. This ID is then usually used as an enricher of the application logs 
(see also [Log4J MDC](https://logging.apache.org/log4j/2.x/manual/thread-context.html)). 

Implementing the above mentioned solutions and having logs and events in your log aggregation facility will allow you to have an incredibly powerful view of your system behavior.


## Status

The project is still incomplete


## Features

* Simple command line application deployed as a __Docker Container__
* Designed to support __workload sharing__ through the deployment of multiple instances
* Aggregates events from __multiple topics__
* Supports writing to Elasticsearch with both __native Elasticsearch__ protocol or __REST over HTTP__ protocol (the last one is required if you are trying to integrate with the AWS managed Elasticsearch facility)
* Events are assumed to be written in __AVRO format__, you could extend the tool to support different format and I will be more than happy to merge your PR
* AVRO Schemas are assumed to be managed via __Confluent's Schema Registry__
* Connection to Elasticsearch is protected via a __Circuit Breaker__ (connection to the Schema Registry is not since the current client doesn't suport)
* The target Elasticsearch index where the events are stored to is __changed every day__
* Configurable path for event timestamp and ID extraction
* Container status can be monitored via an __HTTP Health Check Endpoint__

## Extensions

A series of possible extension I might work on and I will be happy to accept PRs for are:

* Supporting different event serialization formats (Protbuf, Thrift, ...)
* Supporting different log aggregation targets (SolR, ...)
* Supporting different event bus (Kinesis, ...) (_this might be more effort considering current implementation_)

## Technologies

The tool is based on

* Scala 2.11 (would migrate to Scala 2.12 when the libraries used will be available)
* [Akka 2.4](http://akka.io/) 
* [Reactive KAFKA](https://github.com/akka/reactive-kafka) and [AKKA Streams](http://doc.akka.io/docs/akka/2.4.14/scala/stream/stream-introduction.html)
* [Elastic4s](https://github.com/sksamuel/elastic4s)
* [Confluent Schema Registry](http://docs.confluent.io/2.0.0/schema-registry/docs/index.html)



