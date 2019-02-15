package io.klira.franz.engine.plugins.kafka

import io.klira.franz.BasicJob
import io.klira.franz.Consumer
import io.klira.franz.Job
import io.klira.franz.engine.ConsumerPlugin
import io.klira.franz.engine.ConsumerPluginLoadStatus
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue

class KafkaConsumerPlugin(private val options: Map<String, Any>,
                          private val topics: List<String>) : ConsumerPlugin {
    constructor() : this(emptyMap(), emptyList())

    private var consumer: KafkaConsumer<ByteArray, ByteArray>? = null
    private val topicPartitionChangesPending = ConcurrentLinkedQueue<Pair<TopicPartition, Boolean>>()
    private val onRebalance = object : ConsumerRebalanceListener {
        override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {
            if (partitions == null) {
                return
            }
            partitions
                    .map { it to true }
                    .let { topicPartitionChangesPending.addAll(it) }
        }

        override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
            if (partitions == null) {
                return
            }
            partitions
                    .map { it to false }
                    .let { topicPartitionChangesPending.addAll(it) }
        }
    }

    override fun onPluginLoaded(c: Consumer): ConsumerPluginLoadStatus {
        val fullConfig = DEFAULT_OPTIONS + options
        if (required.any { fullConfig.containsKey(it) }) {
            val missingFields = required - fullConfig.keys
            return ConsumerPluginLoadStatus.ConfigurationError("Missing required kafka configuration fields: ${missingFields}")
        }
        consumer = KafkaConsumer(fullConfig)
        c.setPluginMeta("kafkaConsumer", consumer!!)
        return ConsumerPluginLoadStatus.Success
    }

    override fun beforeStarting(c: Consumer) {
        consumer!!.subscribe(topics, onRebalance)
    }

    override fun produceJobs(): List<Job> =
            consumer!!.poll(Duration.ofSeconds(10))
                    .asSequence()
                    .map { BasicJob(KMessage(it)) }
                    .toList()

    override fun onClose() {
        consumer!!.close()
    }

    companion object {
        private val DEFAULT_OPTIONS = mapOf<String, Any>(
                "key.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                "value.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                "bootstrap.servers" to "kafka://localhost:9091"
        )
        private val required: Set<String> = setOf(
                "bootstrap.servers",
                "group.id",
                "key.deserializer",
                "value.deserializer"
        )
        /*
        Defaults according to Anton
        val default: Map<String, Any> = mapOf(
                    "enable.auto.commit" to false,
                    "bootstrap.servers" to (System.getenv("KAFKA_HOST") ?: "localhost:9092"),
                    "key.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                    "value.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                    "heartbeat.interval.ms" to 4_000,
                    "session.timeout.ms" to 12_000,
                    "auto.offset.reset" to "earliest",
                    "max.poll.records" to 100
            )
         */
    }

}