package io.klira.franz

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
    //val (sp, th) = Supervisor.spawnInThread()
    val config = """
        runtime:
          supervisor:
            tasks:
            - worker:
                className: io.klira.franz.MyWorker
              plugins:
              - className: io.klira.franz.engine.plugins.kafka.CommitManager
              - className: io.klira.franz.engine.plugins.kafka.KafkaConsumerPlugin
              - className: io.klira.franz.engine.plugins.retry.RetryManager
    """.trimIndent()
    Runtime.fromString(config).run()
    /*
    sp.createPrototype(MyWorker::class.java) {
        listOf(
                CommitManager(),
                RetryManager(),
                KafkaConsumerPlugin(emptyMap(), listOf("hello", "world"))
        )
    }*/

    //th.join()
}