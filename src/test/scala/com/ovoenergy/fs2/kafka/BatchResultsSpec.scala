package com.ovoenergy.fs2.kafka

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

class BatchResultsSpec extends BaseUnitSpec {

  "BatchResults" when {
    "is empty" should {
      "not contain any results and commits" in {
        val underTest = BatchResults.empty[Int]

        underTest.results shouldBe empty
        underTest.toCommit shouldBe empty
      }

    }

    "one PartitionResult is added" should {
      "contain the PartitionResult result and offset" in {
        val topicPartition = new TopicPartition("test-1", 0)
        val offsetAndMetadata = new OffsetAndMetadata(34)
        val results = Vector(1, 2, 3)

        val underTest = BatchResults
          .empty[Int] :+ PartitionResults(topicPartition, offsetAndMetadata, results)

        underTest.toCommit shouldBe Map(topicPartition -> offsetAndMetadata)
        underTest.results shouldBe results
      }
    }

    "two PartitionResults are added" should {
      "contain the PartitionResults results and offsets" in {

        val one = PartitionResults(
          new TopicPartition("test-1", 1),
          new OffsetAndMetadata(34),
          Vector(1, 2, 3)
        )

        val two = PartitionResults(
          new TopicPartition("test-1", 2),
          new OffsetAndMetadata(76),
          Vector(2, 4, 6)
        )

        val underTest = BatchResults.empty[Int] :+ one :+ two

        underTest.toCommit shouldBe Map(
          one.topicPartition -> one.offset,
          two.topicPartition -> two.offset)
        underTest.results should contain theSameElementsInOrderAs one.results ++ two.results
      }
    }
  }

}
