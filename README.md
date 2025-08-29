# Java Client

This project contains a Java application that subscribes to a topic on a Confluent Cloud Kafka cluster and sends a sample message, then consumes it and prints the consumed record to the console.

## Prerequisites

This project assumes that you already have:
- A Linux/UNIX environment. If you are using Windows, see the tutorial below in the "Learn More" section to download WSL.
- [Java 21](https://www.oracle.com/java/technologies/downloads/#java21) and [Gradle 8.7](https://gradle.org/install/) installed. To confirm both the Gradle version and Java version, run `gradle --version`.

## Installation

You can compile this project by running the following command in the root directory of this project:

```shell
gradle build
```

## Usage

You can run the application by running the following command in the root directory of this project:

```shell
gradle run
```

## Learn more

- For the Java client API, check out the [kafka-clients documentation](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/index.html)
- Check out the full [getting started tutorial](https://developer.confluent.io/get-started/java/)

