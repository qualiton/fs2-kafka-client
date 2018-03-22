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
      "contain the given result and offset" in {
        val underTest = PartitionResults.empty[Int](
          new TopicPartition("test-1", 0),
          new OffsetAndMetadata(9)) :+ RecordResult(1,
                                                    new OffsetAndMetadata(123))

        underTest.results should contain only 1
        underTest.offset shouldBe new OffsetAndMetadata(123)
      }

      "retain topic and partition" in {

        val initial =
          PartitionResults.empty[Int](new TopicPartition("test-1", 0),
                                      new OffsetAndMetadata(9))

        val underTest = initial :+ RecordResult(1, new OffsetAndMetadata(123))

        underTest.topicPartition shouldBe initial.topicPartition
      }
    }

    "two results are added" should {
      "contain all the results in order" in {
        val initial =
          PartitionResults.empty[Int](new TopicPartition("test-1", 0),
                                      new OffsetAndMetadata(9))

        val resultOne = RecordResult(1, new OffsetAndMetadata(123))
        val resultTwo = RecordResult(2, new OffsetAndMetadata(124))

        val underTest = initial :+ resultOne :+ resultTwo

        underTest.results should contain theSameElementsInOrderAs List(1, 2)
      }

      "contain the last result offset" in {
        val initial =
          PartitionResults.empty[Int](new TopicPartition("test-1", 0),
                                      new OffsetAndMetadata(9))

        val resultOne = RecordResult(1, new OffsetAndMetadata(123))
        val resultTwo = RecordResult(2, new OffsetAndMetadata(124))

        val underTest = initial :+ resultOne :+ resultTwo

        underTest.offset shouldBe resultTwo.offset
      }
    }
  }
}
