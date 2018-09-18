/*
 * Copyright 2018 OVO Energy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ovoenergy.fs2.kafka

import scala.concurrent.duration.Duration

/**
  * Contain the settings for Kafka consumer.
  *
  * @param pollTimeout The time to block the thread to poll from kafka waiting for new records.
  * @param maxParallelism The maximum number of topic/partition pair to process in parallel
  * @param nativeSettings The native Kafka consumer settings. To see what is available take a look into
  *                       org.apache.kafka.clients.consumer.ConsumerConfig class.
  */
case class ConsumerSettings(pollTimeout: Duration,
                            maxParallelism: Int,
                            nativeSettings: Map[String, AnyRef])
