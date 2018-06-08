package com.ovoenergy.fs2

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

package object kafka extends Consuming with Producing {
  type Offsets = Map[TopicPartition, OffsetAndMetadata]
}
