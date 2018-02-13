package com.ovoenergy.fs2.kafka

import cats.effect.{Async, Effect, Sync}
import cats.syntax.all._
import fs2._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

trait Consuming {

  private val log = LoggerFactory.getLogger(getClass)

  def consume[F[_]: Sync, K, V](
      subscription: Subscription,
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V],
      settings: ConsumerSettings): Stream[F, ConsumerRecord[K, V]] = {

    consumerStream[F, K, V](keyDeserializer, valueDeserializer, settings)
      .flatMap { consumer =>
        batchStream(consumer, subscription, settings)
          .flatMap { batch =>
            Stream.emits(batch.asScala.toVector)
          }
      }
  }

  def consumeProcessAndCommit[F[_]: Effect, K, V, O](
      subscription: Subscription,
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V],
      settings: ConsumerSettings,
      processRecord: ConsumerRecord[K, V] => F[O])(
      implicit ec: ExecutionContext): Stream[F, O] = {

    consumerStream[F, K, V](keyDeserializer, valueDeserializer, settings)
      .flatMap { consumer =>
        batchStream(consumer, subscription, settings)
          .flatMap { batch =>
            processBatchAndCommit(consumer)(batch,
                                            processRecord,
                                            settings.parallelism)
          }
      }
  }

  def consumerStream[F[_]: Sync, K, V](
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V],
      settings: ConsumerSettings): Stream[F, Consumer[K, V]] = {

    Stream.bracket(
      initConsumer[F, K, V](settings.nativeSettings,
                            keyDeserializer,
                            valueDeserializer))(c => Stream.emit(c).covary[F],
                                                c => closeConsumer(c))

  }

  private def initConsumer[F[_]: Sync, K, V](
      nativeSettings: Map[String, AnyRef],
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]): F[Consumer[K, V]] = Sync[F].delay {
    val consumer = new KafkaConsumer[K, V](nativeSettings.asJava,
                                           keyDeserializer,
                                           valueDeserializer)

    log.debug(s"Consumer initiated with kafkaConsumerSettings: $nativeSettings")

    consumer
  }

  private def closeConsumer[F[_]: Sync, K, V](c: Consumer[K, V]): F[Unit] = {
    Sync[F].delay(c.close())
  }

  private def batchStream[F[_]: Sync, K, V](
      consumer: Consumer[K, V],
      subscription: Subscription,
      settings: ConsumerSettings): Stream[F, ConsumerRecords[K, V]] = {

    for {
      _ <- Stream.eval(subscribeConsumer(consumer, subscription))
      batch <- Stream
        .repeatEval(Sync[F].delay(consumer.poll(settings.pollTimeout.toMillis)))
        .filter(_.count() > 0)
    } yield batch

  }

  private def subscribeConsumer[F[_]: Sync, K, V](
      consumer: Consumer[K, V],
      subscription: Subscription): F[Unit] = Sync[F].delay {
    subscription match {
      case TopicSubscription(topics) => consumer.subscribe(topics.asJava)
    }
  }

  private def processBatchAndCommit[F[_]: Effect, K, V, O](
      consumer: Consumer[K, V])(
      batch: ConsumerRecords[K, V],
      f: ConsumerRecord[K, V] => F[O],
      parallelism: Int)(implicit ec: ExecutionContext): Stream[F, O] = {

    val partitionStreams = batch.partitions.asScala.toSeq.map { tp =>
      val records = batch.records(tp).asScala
      val offsetAndMetadata = new OffsetAndMetadata(records.last.offset())

      Stream
        .emits(records)
        .evalMap(f)
        .fold(PartitionResults.empty[O](tp, offsetAndMetadata))(_ :+ _)
    }

    Stream
      .emits(partitionStreams)
      .join(parallelism)
      .fold(BatchResults.empty[O])(_ :+ _)
      .evalMap { batchResults =>
        commit[F](consumer, batchResults.toCommit)
          .map(_ => batchResults.results)
      }
      .flatMap(Stream.emits(_))
  }

  private def commit[F[_]: Async](
      consumer: Consumer[_, _],
      commits: Map[TopicPartition, OffsetAndMetadata])
    : F[Map[TopicPartition, OffsetAndMetadata]] = {
    // We need to use synchronous commit, as the async one is buffering the calls and it will not call the callback
    // before another commitAsync is invoked.
    Async[F].delay {
      consumer.commitSync(commits.asJava)
      log.debug(s"Offset committed kafkaCommittedOffsets: ${commits}")
      commits
    }
  }

}
