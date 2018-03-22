package com.ovoenergy.fs2.kafka

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

private[kafka] case class PartitionResults[O](topicPartition: TopicPartition,
                                              offset: OffsetAndMetadata,
                                              results: Vector[O]) {
  def :+(o: RecordResult[O]): PartitionResults[O] =
    copy(results = results :+ o.result, offset = o.offset)
}

private[kafka] object PartitionResults {

  def empty[O](topicPartition: TopicPartition,
               offset: OffsetAndMetadata): PartitionResults[O] =
    PartitionResults(topicPartition, offset, Vector.empty[O])
}
