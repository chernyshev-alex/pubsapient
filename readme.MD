
## Prerequisites

- java  1.8
- scala  2.13.8
- spark   3.2.1

Run /usr/libexec/java_home -V and select java 1.8
> export JAVA_HOME=`/usr/libexec/java_home -v 1.8_161`

Warning: Spark 3.2.1 won't work with Java 18.0.2.1 (x86_64) "Homebrew" - "OpenJDK 18.0.2.1"  

## Requirements

CampaignsBatchSpec.scala - spark unit tests for Requests 1, 2

CampaignStreamsReports.sql - KSQL examples

## Data quality

DataQualitySpec. Based on Amazon deequ-1.0.4.jar

## How to run

> $> sbt test

## Documentation 
> ./docs/pubsapient-sd.pdf  - System design proposal


