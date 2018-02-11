package com.ovoenergy.fs2.kafka

import cats.effect.{Async, Sync}
import fs2._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.Serializer

import scala.collection.JavaConverters._

trait Producing {

  def produce[F[_]: Async, K, V](settings: ProducerSettings,
                                 keySerializer: Serializer[K],
                                 valueSerializer: Serializer[V])
  : Pipe[F, ProducerRecord[K, V], RecordMetadata] = {
    record: Stream[F, ProducerRecord[K, V]] =>

      producerStream(settings,keySerializer, valueSerializer)
        .repeat
        .zip(record)
        .evalMap {
          case (p, pr) =>
            produceRecord(p)(pr)
        }
  }

  def producerStream[F[_]: Sync, K, V](settings: ProducerSettings,
                                       keySerializer: Serializer[K],
                                       valueSerializer: Serializer[V]): Stream[F, Producer[K, V]] = {

    Stream
      .bracket(
        initProducer[F, K, V](settings.nativeSettings,
          keySerializer,
          valueSerializer))(
        p => Stream.emit(p).covary[F],
        p => Sync[F].delay(p.close())
      )
  }

  private def initProducer[F[_]: Sync, K, V](
                                              nativeSettings: Map[String, AnyRef],
                                              keySerializer: Serializer[K],
                                              valueSerializer: Serializer[V]): F[Producer[K, V]] = Sync[F].delay {
    new KafkaProducer[K, V](nativeSettings.asJava,
      keySerializer,
      valueSerializer)
  }

  def produceRecord[F[_]: Async, K, V](producer: Producer[K, V])(
    record: ProducerRecord[K, V]): F[RecordMetadata] = {
    Async[F].async[RecordMetadata] { cb =>
      producer.send(
        record,
        (metadata: RecordMetadata, exception: Exception) => Option(exception) match {
          case Some(e) => cb(Left(e))
          case None => cb(Right(metadata))
        }
      )

      ()
    }
  }


}
