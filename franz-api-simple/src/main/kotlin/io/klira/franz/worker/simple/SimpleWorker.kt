package  io.klira.franz.worker.simple

import io.klira.franz.*
import java.lang.RuntimeException

abstract class SimpleWorker : Worker {
    override fun batchType(): JobBatchType {
        return JobBatchType.SINGLE
    }
    private val PROCEED = BasicJobUpdate(true)
    private val RETRY = BasicJobUpdate(false)

    private class GiveUpException internal constructor(inner: Throwable) : Exception(inner)

    protected fun giveUp(t: Throwable): Nothing {
        throw GiveUpException(t)
    }
    protected fun giveUp(msg: String): Nothing {
        giveUp(RuntimeException(msg))
    }
    protected inline fun <T> giveUpIfFails(fn: () -> T): T  {
        try {
            return fn()
        } catch (ex: Throwable) {
            giveUp(ex)
        }
    }
    override suspend fun processBatch(jobBatch: JobBatch): List<Pair<Job, JobUpdate>> {
        val job = jobBatch.asSingle()
        val msg = job.message()
        val update = Result.success(msg)
                .mapCatching {
                    performWork(it)
                }
                .map { PROCEED }
                .recover {
                    if (it is GiveUpException)
                        PROCEED
                    else RETRY
                }
                .getOrThrow()
        return listOf(job to update)
    }

    abstract suspend fun performWork(message: Message)
}