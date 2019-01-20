package io.klira.franz.engine.plugins.kafka

import org.apache.kafka.common.TopicPartition
import java.util.*

class OffsetManager {
    private val partitions = mutableMapOf<TopicPartition, PartitionOffsetManager>()

    private fun get(tp: TopicPartition): PartitionOffsetManager =
            partitions.computeIfAbsent(tp) {
                PartitionOffsetManager()
            }

    fun onFailed(tp: TopicPartition, offset: Long) {
        get(tp).onFailed(offset)
    }
    fun onSuccess(tp: TopicPartition, offset: Long) : Optional<Pair<TopicPartition, Long>> =
        get(tp).onSuccess(offset).map { tp to it }

    fun removeTopicPartition(tp: TopicPartition) {
        partitions.remove(tp)
    }
}