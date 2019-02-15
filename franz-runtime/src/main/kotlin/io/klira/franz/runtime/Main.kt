package io.klira.franz.runtime

import io.klira.franz.BasicJobUpdate
import io.klira.franz.Job
import io.klira.franz.JobUpdate
import io.klira.franz.Worker

class MyWorker : Worker {
    override suspend fun processMessage(job: Job): JobUpdate {
        return BasicJobUpdate(true)
    }
}


fun main(args: Array<String>) {
    Runtime.fromFile("franz.yaml").run()
}