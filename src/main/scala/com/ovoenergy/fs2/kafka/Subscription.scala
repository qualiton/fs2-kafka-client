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

import cats.data.NonEmptyList
import cats.Show

/**
  * Represents a subscription to a Kafka. It could be to:
  *  - A set of topics
  *  - A topic name pattern
  *  - A specific partition
  *  - A specific offset of a partition
  *
  *  At the moment only the first case is implemented.
  */
sealed trait Subscription

case class TopicSubscription(topics: Set[String]) extends Subscription

object Subscription {

  implicit val subscriptionShow: Show[Subscription] = Show.show {
    case TopicSubscription(topics) => topics.mkString("Topics[", ",", "]")
  }

  /**
    * Create a Subscription to the given topics.
    */
  def topics(topic: String, rest: String*): Subscription =
    topics(NonEmptyList(topic, rest.toList))

  /**
    * Create a Subscription to the given topics.
    */
  def topics(topics: NonEmptyList[String]): Subscription =
    TopicSubscription(topics.toList.toSet)
}
