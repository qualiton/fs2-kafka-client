package com.ovoenergy.fs2.kafka

import org.apache.kafka.clients.consumer.OffsetAndMetadata

private[kafka] case class RecordResult[A](result: A, offset: OffsetAndMetadata)
