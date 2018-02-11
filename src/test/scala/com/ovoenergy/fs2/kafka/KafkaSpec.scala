package com.ovoenergy.fs2.kafka

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{Matchers, WordSpec}
import cats.effect.IO
import org.apache.kafka.clients.consumer.{
  ConsumerConfig,
  ConsumerRecord,
  KafkaConsumer
}
import org.apache.kafka.clients.producer.{
  ProducerConfig,
  ProducerRecord,
  RecordMetadata
}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._

class KafkaSpec extends WordSpec with Matchers with EmbeddedKafka {

  implicit val stringSerializer: Serializer[String] = new StringSerializer
  implicit val stringDeserializer: Deserializer[String] = new StringDeserializer

  "consume" should {
    "return a Stream of kafka messages" in withRunningKafkaOnFoundPort(
      EmbeddedKafkaConfig()) { config =>
      val topic = "test-1"
      val groupId = "test-1"

      val settings = ConsumerSettings(
        250.milliseconds,
        4,
        Map(
          ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ConsumerConfig.GROUP_ID_CONFIG -> groupId,
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
        )
      )

      createCustomTopic(topic, partitions = 3)

      val produced = (0 until 1000).map(i => s"key-$i" -> s"value->$i")

      publishToKafka(topic, produced)

      val consumed = consume[IO, String, String](TopicSubscription(Set(topic)),
                                                 new StringDeserializer,
                                                 new StringDeserializer,
                                                 settings)
        .take(1000)
        .map(r => r.key() -> r.value())
        .compile
        .toVector
        .unsafeRunSync()

      consumed should contain theSameElementsAs produced

    }
  }

  "consumeProcessAndCommit" should {
    "commit all the messages" in withRunningKafkaOnFoundPort(
      EmbeddedKafkaConfig(customConsumerProperties =
        Map(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG -> "false"))) {
      config =>
        val topic = "test-1"
        val groupId = "test-1"

        val settings = ConsumerSettings(
          250.milliseconds,
          4,
          Map(
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
            ConsumerConfig.GROUP_ID_CONFIG -> groupId,
            ConsumerConfig.GROUP_ID_CONFIG -> groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
          )
        )

        createCustomTopic(topic, partitions = 3)

        val produced = (0 until 100).map(i => s"key-$i" -> s"value->$i")
        publishToKafka(topic, produced)

        val consumeStream =
          consumeProcessAndCommit[IO, String, String, (Int, Long)](
            TopicSubscription(Set(topic)),
            new StringDeserializer,
            new StringDeserializer,
            settings, { x =>
              IO(x.partition() -> x.offset())
            }
          ).take(100)

        val committedOffsets =
          consumeStream.compile.toVector.unsafeRunSync().groupBy(_._1).map {
            case (p, offsets) =>
              p -> offsets.map(_._2).max
          }

        withKafkaConsumer[String, String, Unit](settings) { consumer =>
          committedOffsets.foreach {
            case (partition, offset) =>
              consumer
                .committed(new TopicPartition(topic, partition))
                .offset() shouldBe offset
          }
        }
    }
  }

  "produce" should {
    "be composed with consumeProcessAndCommit" in withRunningKafkaOnFoundPort(
      EmbeddedKafkaConfig()) { config =>
      {

        val sourceTopic = "source"
        val destinationTopic = "destination"

        val groupId = "test-1"

        val consumerSettings = ConsumerSettings(
          250.milliseconds,
          4,
          Map(
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
            ConsumerConfig.GROUP_ID_CONFIG -> groupId,
            ConsumerConfig.GROUP_ID_CONFIG -> groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
          )
        )

        val producerSettings = ProducerSettings(
          Map(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
            ProducerConfig.ACKS_CONFIG -> "all",
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> "1"
          )
        )

        createCustomTopic(sourceTopic, partitions = 6)
        createCustomTopic(destinationTopic, partitions = 12)

        val produced = (0 until 1000).map(i => s"key-$i" -> s"value->$i")
        publishToKafka(sourceTopic, produced)

        val stream =
          producerStream[IO, String, String](producerSettings,
                                             stringSerializer,
                                             stringSerializer).flatMap {
            producer =>
              def process(record: ConsumerRecord[String, String])
                : IO[RecordMetadata] = {
                produceRecord[IO, String, String](producer)(
                  new ProducerRecord[String, String](destinationTopic,
                                                     record.key(),
                                                     record.value()))
              }

              consumeProcessAndCommit[IO, String, String, RecordMetadata](
                TopicSubscription(Set(sourceTopic)),
                new StringDeserializer,
                new StringDeserializer,
                consumerSettings, { x =>
                  process(x)
                }
              )
          }

        stream.take(1000).compile.toVector.unsafeRunSync()

        val messages = consumeNumberKeyedMessagesFrom(destinationTopic, 1000, false)

        messages should contain theSameElementsAs produced
      }

    }
  }

  def withKafkaConsumer[K, V, A](settings: ConsumerSettings)(
      f: KafkaConsumer[K, V] => A)(implicit keyDeserializer: Deserializer[K],
                                   valueDeserializer: Deserializer[V]): A = {
    val consumer = new KafkaConsumer[K, V](settings.nativeSettings.asJava,
                                           keyDeserializer,
                                           valueDeserializer)
    try {
      f(consumer)
    } finally {
      consumer.close()
    }

  }

}
