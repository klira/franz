package io.klira.franz

import io.klira.franz.engine.Consumer
import io.klira.franz.engine.plugins.kafka.CommitManager
import io.klira.franz.engine.plugins.kafka.KafkaConsumerPlugin
import io.klira.franz.engine.plugins.retry.RetryManager
import io.klira.franz.impl.BasicJobUpdate
import io.klira.franz.supervisor.Supervisor

class MyWorker : Worker {
    override suspend fun processMessage(job: Job): JobUpdate {
        return BasicJobUpdate(true)
    }
}


fun main(args: Array<String>) {
    val (sp, th) = Supervisor.spawnInThread()

    sp.createPrototype(MyWorker::class.java) {
        listOf(
                CommitManager(),
                RetryManager(),
                KafkaConsumerPlugin(emptyMap(), listOf("hello", "world"))
        )
    }

    th.join()
}