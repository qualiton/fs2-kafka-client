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
