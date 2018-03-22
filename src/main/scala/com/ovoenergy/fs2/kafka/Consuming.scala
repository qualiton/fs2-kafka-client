package com.ovoenergy.fs2.kafka

import cats.effect.{Async, Effect, Sync}
import cats.syntax.all._
import com.ovoenergy.fs2.kafka.Consuming.{
  ConsumePartiallyApplied,
  ConsumeProcessAndCommitPartiallyApplied,
  ConsumerStreamPartiallyApplied
}
import fs2._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

/**
  * The Consuming side of the Kafka client.
  */
trait Consuming {

  /**
    * Consume records from the given subscription and provides a `Stream[F, ConsumerRecord[K, V]]`.
    */
  def consume[F[_]]: ConsumePartiallyApplied[F] = new ConsumePartiallyApplied[F]

  /**
    * Consume records from the given subscription, for each of them apply the given process function and commit back to
    * kafka.
    *
    * The records in each topic/partitionp air are processed in sequence. Each topic/partitionp is processed in parallel
    * up to the given parallelism.
    *
    * The result of the processing is returned as `Stream[F, O]` where O if the return type of the given process
    * function.
    */
  def consumeProcessAndCommit[F[_]]
    : ConsumeProcessAndCommitPartiallyApplied[F] =
    new ConsumeProcessAndCommitPartiallyApplied[F]

  /**
    * Provides a `Stream[F, Consumer[K,V]]` that will automatically close the consumer when completed.
    */
  def consumerStream[F[_]]: ConsumerStreamPartiallyApplied[F] =
    new ConsumerStreamPartiallyApplied[F]

}

object Consuming {

  private val log = LoggerFactory.getLogger(classOf[Consuming])

  private[kafka] final class ConsumePartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V](subscription: Subscription,
                    keyDeserializer: Deserializer[K],
                    valueDeserializer: Deserializer[V],
                    settings: ConsumerSettings)(
        implicit F: Effect[F]): Stream[F, ConsumerRecord[K, V]] = {

      consumerStream[F](keyDeserializer, valueDeserializer, settings)
        .flatMap { consumer =>
          batchStream(consumer, subscription, settings)
            .flatMap { batch =>
              Stream.emits(batch.asScala.toVector)
            }
        }
    }
  }

  private[kafka] final class ConsumeProcessAndCommitPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V, O](subscription: Subscription,
                       keyDeserializer: Deserializer[K],
                       valueDeserializer: Deserializer[V],
                       settings: ConsumerSettings)(
        processRecord: ConsumerRecord[K, V] => F[O])(
        implicit F: Effect[F],
        ec: ExecutionContext): Stream[F, O] = {

      consumerStream[F](keyDeserializer, valueDeserializer, settings)
        .flatMap { consumer =>
          batchStream(consumer, subscription, settings)
            .flatMap { batch =>
              processBatchAndCommit(consumer)(batch,
                                              processRecord,
                                              settings.maxParallelism)
            }
        }
    }
  }

  private[kafka] final class ConsumerStreamPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {

    def apply[K, V](keyDeserializer: Deserializer[K],
                    valueDeserializer: Deserializer[V],
                    settings: ConsumerSettings)(
        implicit F: Sync[F]): Stream[F, Consumer[K, V]] = {

      Stream.bracket(
        initConsumer[F, K, V](settings.nativeSettings,
                              keyDeserializer,
                              valueDeserializer))(c => Stream.emit(c).covary[F],
                                                  c => closeConsumer(c))

    }
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

    log.debug(s"Consumer subscribed to ${subscription.show}")
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
    Sync[F].delay(c.close()) >> Sync[F].delay(log.debug(s"Consumer closed"))
  }

  private def processBatchAndCommit[F[_]: Effect, K, V, O](
      consumer: Consumer[K, V])(
      batch: ConsumerRecords[K, V],
      f: ConsumerRecord[K, V] => F[O],
      parallelism: Int)(implicit ec: ExecutionContext): Stream[F, O] = {

    log.debug(s"Processing batch $batch")

    val partitionStreams = batch.partitions.asScala.toSeq.map { tp =>
      val records = batch.records(tp).asScala
      val offsetAndMetadata = new OffsetAndMetadata(records.head.offset())

      Stream
        .emits(records)
        .evalMap { record =>
          f(record).map(result =>
            RecordResult(result, new OffsetAndMetadata(record.offset() + 1)))
        }
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
    // before another commitAsync or commitSync is invoked.
    Async[F].delay {
      consumer.commitSync(commits.asJava)
      log.debug(s"Offset committed kafkaCommittedOffsets: $commits")
      commits
    }
  }
}
