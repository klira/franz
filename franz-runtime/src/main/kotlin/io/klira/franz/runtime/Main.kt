package io.klira.franz.runtime

import io.klira.franz.*

class MyWorker : Worker {
    override suspend fun processBatch(jobBatch: JobBatch): List<Pair<Job, JobUpdate>> {
        val j = jobBatch.asSingle()
        return listOf(j to BasicJobUpdate(true))
    }
}


fun main(args: Array<String>) {
    Runtime.fromFile("franz.yaml").run()
}