package com.ovoenergy.fs2.kafka

import scala.concurrent.duration.Duration

/**
  * Contain the settings for Kafka consumer.
  *
  * @param pollTimeout The time to block the thread to poll from kafka waiting for new records.
  * @param maxParallelism The maximum number of topic/partition pair to process in parallel
  * @param nativeSettings The native Kafka consumer settings. To see what is available take a look into
  *                       [[org.apache.kafka.clients.consumer.ConsumerConfig]] class.
  */
case class ConsumerSettings(pollTimeout: Duration,
                            maxParallelism: Int,
                            nativeSettings: Map[String, AnyRef])
