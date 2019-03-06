package io.klira.franz.engine.plugins.kafka

import io.klira.franz.BasicJob
import io.klira.franz.Consumer
import io.klira.franz.JobBatch
import io.klira.franz.JobBatches
import io.klira.franz.engine.ConsumerPlugin
import io.klira.franz.engine.ConsumerPluginLoadStatus
import io.klira.franz.engine.ConsumerPluginOptions
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue

class KafkaConsumerPlugin(pluginOptions: ConsumerPluginOptions) : ConsumerPlugin {
    private val meta = pluginOptions.metadata
    private val options: Map<String, Any> = pluginOptions.getMap("kafkaOptions")
    private val topics: List<String> = pluginOptions.options["topics"] as? List<String> ?: emptyList()

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

    private fun buildSmartDefaults() : Map<String, Any> {
        val opts = mutableMapOf<String, Any>()
        meta.name?.let {
            opts["group.id"] = it
        }
        System.getenv("KAFKA_URL")?.let {
            opts["bootstrap.servers"] = it
        }
        return opts.toMap()
    }
    override fun onPluginLoaded(c: Consumer): ConsumerPluginLoadStatus {
        val fullConfig = DEFAULT_OPTIONS + buildSmartDefaults() + options
        if (required.any { !fullConfig.containsKey(it) }) {
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

    override fun produceJobs(): JobBatch {
        val pollRes = consumer!!.poll(Duration.ofSeconds(10))

        if (pollRes.isEmpty) {
            return JobBatches.empty()
        }
        val partitions = pollRes.partitions()
        val tagged = partitions
                .map {
                    it as Any to
                            pollRes.records(it).map { rec -> BasicJob(KMessage(rec)) }
                }
                .toMap()
        return JobBatches.fromTagged(tagged)
    }

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