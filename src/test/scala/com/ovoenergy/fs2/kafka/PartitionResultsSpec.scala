package com.ovoenergy.fs2.kafka

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

class PartitionResultsSpec extends BaseUnitSpec {

  "PartitionResults" when {
    "is empty" should {
      "not contain any results" in {
        PartitionResults
          .empty[Int](new TopicPartition("test-1", 0), new OffsetAndMetadata(9))
          .results shouldBe empty
      }
    }

    "a result is added" should {
      "contain the given result" in {
        val underTest = PartitionResults.empty[Int](
          new TopicPartition("test-1", 0),
          new OffsetAndMetadata(9)) :+ 1

        underTest.results should contain only 1
      }

      "retain topic, partition and offset" in {

        val initial =
          PartitionResults.empty[Int](new TopicPartition("test-1", 0),
                                      new OffsetAndMetadata(9))

        val underTest = initial :+ 1

        underTest.topicPartition shouldBe initial.topicPartition
        underTest.offset shouldBe initial.offset
      }
    }

    "two results are added" should {
      "contain all the results in order" in {
        val initial =
          PartitionResults.empty[Int](new TopicPartition("test-1", 0),
                                      new OffsetAndMetadata(9))
        val underTest = initial :+ 1 :+ 2

        underTest.results should contain theSameElementsInOrderAs List(1, 2)
      }
    }
  }
}
