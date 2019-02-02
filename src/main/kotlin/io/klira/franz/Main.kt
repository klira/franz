package io.klira.franz

import io.klira.franz.impl.BasicJobUpdate

class MyWorker : Worker {
    override suspend fun processMessage(job: Job): JobUpdate {
        return BasicJobUpdate(true)
    }
}


fun main(args: Array<String>) {
    Runtime.fromFile("franz.yaml").run()
}