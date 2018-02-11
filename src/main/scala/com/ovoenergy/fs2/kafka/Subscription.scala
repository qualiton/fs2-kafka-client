package com.ovoenergy.fs2.kafka

sealed trait Subscription

case class TopicSubscription(topics: Set[String]) extends Subscription

