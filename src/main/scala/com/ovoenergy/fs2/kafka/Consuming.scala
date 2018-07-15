package com.ovoenergy.fs2.kafka

import cats.effect.{Async, Effect, Sync}
import cats.syntax.all._
import com.ovoenergy.fs2.kafka.Consuming._
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
    * Consume records from the given subscription, and apply the provided function on each record,
    * and commit the offsets to Kafka. The records in each topic/partition will be processed in
    * sequence, while multiple topic/partitions will be processed in parallel, up to the
    * specified parallelism.
    *
    * The result of the processing is a `Stream[F, O]` where `O` is the
    * return type of the provided function.
    */
  def consumeProcessAndCommit[F[_]]
    : ConsumeProcessAndCommitPartiallyApplied[F] =
    new ConsumeProcessAndCommitPartiallyApplied[F]

  /**
    * Consume records from the given subscription, and apply the provided function on each batch
    * of records, and commit the offsets to Kafka. The records in each topic/partition will be
    * processed in sequence, while multiple topic/partitions will be processed in parallel,
    * up to the specified parallelism.
    *
    * The result of the processing is a `Stream[F, O]` where `O` is the
    * return type of the provided function.
    */
  def consumeProcessBatchAndCommit[F[_]]
    : ConsumeProcessBatchAndCommitPartiallyApplied[F] =
    new ConsumeProcessBatchAndCommitPartiallyApplied[F]

  /**
    * Consume records from the given subscription, and apply the provided `Pipe[F[_], ConsumerRecord[K, V], BatchProcessed.type]` function on each batch
    * of records which is converted to stream, and commit the offsets to Kafka after the stream returned with BatchProcessed.
    * The complete error handling is delegated to the provided pipe. When the stream returns with BatchProcessed.type it is assumed that every emitted element was processed
    * and the latest offset in the batch will be used for commit.
    * The records in each topic/partition will be processed in sequence, while multiple topic/partitions will be processed in parallel,
    * up to the specified parallelism.
    *
    * The result of the processing is a `Stream[F, Map[TopicPartition, OffsetAndMetadata]`
    */
  def consumeProcessBatchWithPipeAndCommit[F[_]]
    : ConsumeProcessBatchWithPipeAndCommitPartiallyApplied[F] =
    new ConsumeProcessBatchWithPipeAndCommitPartiallyApplied[F]

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

  private[kafka] final class ConsumeProcessBatchAndCommitPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V, O](subscription: Subscription,
                       keyDeserializer: Deserializer[K],
                       valueDeserializer: Deserializer[V],
                       settings: ConsumerSettings)(
        processRecordBatch: Chunk[ConsumerRecord[K, V]] => F[
          Chunk[(O, Offset)]])(implicit F: Effect[F],
                               ec: ExecutionContext): Stream[F, O] = {

      consumerStream[F](keyDeserializer, valueDeserializer, settings)
        .flatMap { consumer =>
          batchStream(consumer, subscription, settings)
            .flatMap { batch =>
              processBatchChunkAndCommit(consumer)(batch,
                                                   processRecordBatch,
                                                   settings.maxParallelism)
            }
        }
    }
  }

  private[kafka] final class ConsumeProcessBatchWithPipeAndCommitPartiallyApplied[
      F[_]](val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V](subscription: Subscription,
                    keyDeserializer: Deserializer[K],
                    valueDeserializer: Deserializer[V],
                    settings: ConsumerSettings)(
        processRecordBatch: Pipe[F, ConsumerRecord[K, V], BatchProcessed.type])(
        implicit F: Effect[F],
        ec: ExecutionContext)
      : Stream[F, Map[TopicPartition, OffsetAndMetadata]] = {

      consumerStream[F](keyDeserializer, valueDeserializer, settings)
        .flatMap { consumer =>
          batchStream(consumer, subscription, settings)
            .flatMap { batch =>
              processBatchWithPipeAndCommit(consumer)(batch,
                                                      processRecordBatch,
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

  private def processBatchChunkAndCommit[F[_]: Effect, K, V, O](
      consumer: Consumer[K, V])(
      batch: ConsumerRecords[K, V],
      f: Chunk[ConsumerRecord[K, V]] => F[Chunk[(O, Offset)]],
      parallelism: Int)(implicit ec: ExecutionContext): Stream[F, O] = {

    log.debug(s"Processing batchChunk $batch")

    val partitionStreams = batch.partitions.asScala.toSeq.map { tp =>
      val records = batch.records(tp).asScala
      val offsetAndMetadata = new OffsetAndMetadata(records.head.offset())

      Stream
        .eval(f(Chunk.seq(records)))
        .flatMap { chunk =>
          Stream
            .chunk(chunk)
            .map {
              case (result, Offset(offset)) =>
                RecordResult(result, new OffsetAndMetadata(offset + 1L))
            }
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

  private def processBatchWithPipeAndCommit[F[_]: Effect, K, V](
      consumer: Consumer[K, V])(
      batch: ConsumerRecords[K, V],
      pipe: Pipe[F, ConsumerRecord[K, V], BatchProcessed.type],
      parallelism: Int)(implicit ec: ExecutionContext)
    : Stream[F, Map[TopicPartition, OffsetAndMetadata]] = {

    log.debug(s"Processing batch with pipe $batch")

    val partitionStreams = batch.partitions.asScala.toSeq.map { tp =>
      val records = batch.records(tp).asScala

      Stream
        .emits(records)
        .covary[F]
        .through(pipe)
        .map(
          r =>
            PartitionResults(tp,
                             new OffsetAndMetadata(records.last.offset() + 1L),
                             Vector(r)))
        .observe1(pr =>
          Effect[F].delay(log.debug(
            s"Commit offset:${pr.topicPartition.toString}:${pr.offset} - batch size:${records.size}")))
    }

    Stream
      .emits(partitionStreams)
      .join(parallelism)
      .fold(BatchResults.empty[BatchProcessed.type])(_ :+ _)
      .evalMap { batchResults =>
        commit[F](consumer, batchResults.toCommit)
      }
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
