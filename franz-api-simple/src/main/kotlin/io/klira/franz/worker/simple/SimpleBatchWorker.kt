package io.klira.franz.worker.simple

import io.klira.franz.*

abstract class SimpleBatchWorker : Worker {
    private val PROCEED = BasicJobUpdate(true)
    private val RETRY = BasicJobUpdate(false)
    override fun batchType(): JobBatchType {
        return JobBatchType.TAGGGED
    }

    protected fun maxSize(): Int = Int.MAX_VALUE

    override suspend fun processBatch(jobBatch: JobBatch): List<Pair<Job, JobUpdate>> {
        val jobs = jobBatch.asTagged()
        return jobs.flatMap { (k, v) ->
            v.chunked(maxSize())
                    .flatMap { chunk ->
                        val res = Result.success(chunk)
                                .mapCatching {
                                    performWork(k, it.map { it.message() })
                                }
                                .map { PROCEED }
                                .recover { RETRY }
                                .getOrThrow()
                        chunk.map { it to res }
                    }
        }
    }
    abstract suspend fun performWork(key: Any, messages: List<Message>)
}