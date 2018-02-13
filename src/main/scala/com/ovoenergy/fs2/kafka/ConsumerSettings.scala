package com.ovoenergy.fs2.kafka

import scala.concurrent.duration.Duration

case class ConsumerSettings(pollTimeout: Duration,
                            parallelism: Int,
                            nativeSettings: Map[String, AnyRef])
