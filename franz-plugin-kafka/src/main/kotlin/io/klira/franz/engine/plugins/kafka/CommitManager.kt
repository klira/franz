package io.klira.franz.engine.plugins.kafka

import io.klira.franz.Consumer
import io.klira.franz.Job
import io.klira.franz.JobUpdate
import io.klira.franz.Message
import io.klira.franz.engine.ConsumerPlugin
import io.klira.franz.engine.ConsumerPluginOptions
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.common.TopicPartition
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

private fun Message.topicPartition(): TopicPartition =
        TopicPartition(topic(), partition().toInt())

class CommitManager(private val opts: ConsumerPluginOptions) : ConsumerPlugin {
    private val busyCommitting = AtomicBoolean(false)
    private var offsetsToCommit = mutableMapOf<TopicPartition, OffsetAndMetadata>()
    private val offsetManager = OffsetManager()
    private var consumer: KafkaConsumer<ByteArray, ByteArray>? = null
    private val onCommit = OffsetCommitCallback { offsets, exc ->
        busyCommitting.set(false)
    }

    override fun beforeStarting(c: Consumer) {
        consumer = c.getPluginMeta("kafkaConsumer") as KafkaConsumer<ByteArray, ByteArray>
    }

    private fun commitUnlessBusy(consumer: KafkaConsumer<ByteArray, ByteArray>) {
        if (!busyCommitting.get()) {
            commitUnconditionally(consumer)
        }
    }

    private fun commitUnconditionally(consumer: KafkaConsumer<ByteArray, ByteArray>) {
        // Obvious no-op is skipped.
        if (offsetsToCommit.isNotEmpty()) {
            return
        }
        consumer.commitAsync(offsetsToCommit, onCommit)
        offsetsToCommit.clear()
        busyCommitting.set(true)
    }

    override fun onTick() {
        commitUnlessBusy(consumer!!)
    }

    override fun beforeClosing() {
        if (consumer != null) {
            commitUnconditionally(consumer!!)
        } // else: Log something
    }

    private fun <T> Sequence<Optional<T>>.nonEmpty(): Sequence<T> =
            filter { it.isPresent }.map { it.get() }

    override fun handleJobUpdates(results: List<Pair<Job, JobUpdate>>) {
        val newOffsets = results.asSequence()
                .map { (job, update) ->
                    val tp = job.message().topicPartition()
                    if (update.mayAdvanceOffset()) {
                        offsetManager.onSuccess(tp, job.message().offset())
                    } else {
                        offsetManager.onFailed(tp, job.message().offset())
                        Optional.empty()
                    }
                }
                .nonEmpty()
                .groupBy { (a, _) -> a }
                .mapValues { (_, v) -> v.map { (_, x) -> x }.max()!! }
                .mapValues { (_, v) -> OffsetAndMetadata(v) }

        newOffsets.forEach { tp, newOffset ->
            offsetsToCommit.merge(tp, newOffset) { oldOffset, newOffset ->
                if (newOffset.offset() > oldOffset.offset()) newOffset
                else oldOffset
            }
        }
    }
}