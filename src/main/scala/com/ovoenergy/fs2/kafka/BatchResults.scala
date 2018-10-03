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

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

private[kafka] case class BatchResults[O](
    toCommit: Map[TopicPartition, OffsetAndMetadata],
    results: Vector[O]) {
  def :+(pr: PartitionResults[O]): BatchResults[O] =
    copy(toCommit = toCommit + (pr.topicPartition -> pr.offset), results = results ++ pr.results)
}

private[kafka] object BatchResults {
  def empty[O]: BatchResults[O] = BatchResults[O](Map.empty, Vector.empty)
}
