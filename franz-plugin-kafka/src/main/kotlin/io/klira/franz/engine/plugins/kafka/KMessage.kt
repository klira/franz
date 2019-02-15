package io.klira.franz.engine.plugins.kafka

import io.klira.franz.Message
import org.apache.kafka.clients.consumer.ConsumerRecord

class KMessage(private val rec: ConsumerRecord<ByteArray, ByteArray>) : Message {
    override fun offset(): Long = rec.offset()
    override fun value(): ByteArray = rec.value()
    override fun key(): ByteArray = rec.key()
    override fun headers(): Array<Pair<String, ByteArray>> = rec.headers()
            .filterNot { null == it.key() || null == it.value() }
            .map { Pair(it.key()!!, it.value()!!) }
            .toTypedArray()

    override fun headers(key: String): Array<ByteArray> = rec.headers()
            .filter { it.key() == key }
            .map { it.value() }
            .toTypedArray()

    override fun timestamp(): Long = rec.timestamp()
    override fun topic(): String = rec.topic()
    override fun partition(): Long = rec.partition().toLong()
}