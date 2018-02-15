package com.ovoenergy.fs2.kafka

import cats.effect.{Async, Sync}
import com.ovoenergy.fs2.kafka.Producing.{
  ProduceRecordPartiallyApplied,
  ProducerPartiallyApplied,
  ProducerStreamPartiallyApplied
}
import fs2._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.Serializer

import scala.collection.JavaConverters._

/**
  * The Producing side of the Kafka client.
  */
trait Producing {

  /**
    * Provides a `Pipe[F, ProducerRecord[K, V], RecordMetadata]` that will send each record to kafka.
    */
  def produce[F[_]]: ProducerPartiallyApplied[F] =
    new ProducerPartiallyApplied[F]

  /**
    * Provides a `Stream[F, Producer[K,V]]` that will automatically close the producer when completed.
    */
  def producerStream[F[_]]: ProducerStreamPartiallyApplied[F] =
    new ProducerStreamPartiallyApplied[F]

  /**
    * Sends a ProducerRecord[K,V] to Kafka.
    */
  def produceRecord[F[_]]: ProduceRecordPartiallyApplied[F] =
    new ProduceRecordPartiallyApplied[F]

}

object Producing {

  private[kafka] final class ProducerPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V](settings: ProducerSettings,
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V])(
        implicit F: Async[F]): Pipe[F, ProducerRecord[K, V], RecordMetadata] = {

      record: Stream[F, ProducerRecord[K, V]] =>
        producerStream(settings, keySerializer, valueSerializer).repeat
          .zip(record)
          .evalMap {
            case (p, pr) =>
              produceRecord(p, pr)
          }

    }
  }

  private[kafka] final class ProducerStreamPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V](settings: ProducerSettings,
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V])(
        implicit F: Sync[F]): Stream[F, Producer[K, V]] = {

      Stream
        .bracket(
          initProducer[F, K, V](settings.nativeSettings,
                                keySerializer,
                                valueSerializer))(
          p => Stream.emit(p).covary[F],
          p => Sync[F].delay(p.close())
        )

    }
  }

  private[kafka] final class ProduceRecordPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V](producer: Producer[K, V], record: ProducerRecord[K, V])(
        implicit F: Async[F]): F[RecordMetadata] = {

      F.async[RecordMetadata] { cb =>
        producer.send(
          record,
          (metadata: RecordMetadata, exception: Exception) =>
            Option(exception) match {
              case Some(e) => cb(Left(e))
              case None    => cb(Right(metadata))
          }
        )

        ()
      }
    }
  }

  private def initProducer[F[_]: Sync, K, V](
      nativeSettings: Map[String, AnyRef],
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V]): F[Producer[K, V]] = Sync[F].delay {
    new KafkaProducer[K, V](nativeSettings.asJava,
                            keySerializer,
                            valueSerializer)
  }

}
