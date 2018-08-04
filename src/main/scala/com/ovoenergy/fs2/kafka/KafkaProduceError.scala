package com.ovoenergy.fs2.kafka

import org.apache.kafka.clients.producer.ProducerRecord

final case class KafkaProduceError[K, V](record: ProducerRecord[K, V],
                                         error: Throwable)
    extends Throwable(error)
