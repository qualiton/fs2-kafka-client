package com.ovoenergy.fs2.kafka

import org.apache.kafka.clients.producer.ProducerRecord

final case class KafkaProduceException[K, V](record: ProducerRecord[K, V],
                                             cause: Throwable)
    extends Exception(cause)
