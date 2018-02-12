# Simple Kafka client for functional streams for scala
This is a tiny fs2 wrapper around the Kafka java client.  

## Installation
To get started with SBT, simply add the following lines to your build.sbt file.

```sbtshell
libraryDependencies += "com.ovoenergy" %% "fs2-kafka-client" % "<latest version>"
```

## Consuming
To consume records without committing or with auto-commit:
```scala
import com.ovoenergy.fs2.kafka._
import scala.concurrent.duration._
import org.apache.kafka.common.serialization._
import org.apache.kafka.clients.consumer._
import cats.effect.IO

val settings = ConsumerSettings(
    pollTimeout = 250.milliseconds,
    parallelism = 4,
    nativeSettings = Map(
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "my-group-id",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )
)

val consumedRecords = consume[IO, String, String](
  TopicSubscription(Set("my-topic")),
  new StringDeserializer,
  new StringDeserializer,
  settings
).take(1000).compile.toVector.unsafeRunSync()

```

To consume records, apply a function for each record and commit:
```scala
import com.ovoenergy.fs2.kafka._
import scala.concurrent.duration._
import org.apache.kafka.common.serialization._
import org.apache.kafka.clients.consumer._
import cats.effect.IO

val settings = ConsumerSettings(
    pollTimeout = 250.milliseconds,
    parallelism = 4,
    nativeSettings = Map(
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "my-group-id",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )
)

def processRecord(r: ConsumerRecord[String, String]): IO[Int] = IO {
  // Apply some side-effect
  1
}

val ints = consumeProcessAndCommit[IO, String, String, Int](
    TopicSubscription(Set("my-topic")),
    new StringDeserializer,
    new StringDeserializer,
    settings, 
    processRecord
).take(1000).compile.toVector.unsafeRunSync()
```

The record processing order is guaranteed within the same partition, while records from different partitions are processed
in parallel up to the parallelism set into the `ConsumerSettings`.

## Producing
The producer is available as effectfull function:

```scala
import com.ovoenergy.fs2.kafka._
import scala.concurrent.duration._
import org.apache.kafka.common.serialization._
import org.apache.kafka.clients.producer._
import cats.effect.IO


val topic = "my-topic"
val key = "my-key"
val value = "my-value"

val producer: Producer[String, String] = ???

produceRecord[IO, String, String](producer)(new ProducerRecord[String, String](topic, key, value))
```

The producer itself is available trough a `Stream`:
```scala

import com.ovoenergy.fs2.kafka._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization._
import cats.effect.IO
import fs2._

val producerSettings = ProducerSettings(
  Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:9092",
    ProducerConfig.ACKS_CONFIG -> "all",
    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> "1"
  )
)

val topic = "my-topic"

producerStream[IO, String, String](producerSettings,
                                   new StringSerializer,
                                   new StringSerializer).flatMap {producer =>
            
    val key = "my-key"
    val value = "my-value"

    Stream.eval{
      produceRecord[IO, String, String](producer)(
        new ProducerRecord[String, String](topic,key, value))
    }
}
```

