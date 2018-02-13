package com.ovoenergy.fs2.kafka

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

private[kafka] case class BatchResults[O](
    toCommit: Map[TopicPartition, OffsetAndMetadata],
    results: Vector[O]) {
  def :+(pr: PartitionResults[O]): BatchResults[O] =
    copy(toCommit = toCommit + (pr.topicPartition -> pr.offset),
         results = pr.results ++ results)
}

private[kafka] object BatchResults {
  def empty[O]: BatchResults[O] = BatchResults[O](Map.empty, Vector.empty)
}
