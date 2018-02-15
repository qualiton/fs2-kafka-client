package com.ovoenergy.fs2.kafka

/**
  * Contain the Kafka producer settings.
  *
  * @param nativeSettings The native Kafka producer settings. To see what is available take a look into
  *                       [[org.apache.kafka.clients.producer.ProducerConfig]] class.
  */
case class ProducerSettings(nativeSettings: Map[String, AnyRef])
