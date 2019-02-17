package io.klira.franz

interface Worker {
    fun batchType(): JobBatchType = JobBatchType.SINGLE
    suspend fun processBatch(jobBatch: JobBatch): List<Pair<Job, JobUpdate>>
}

