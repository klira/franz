package io.klira.franz.engine.plugins.retry

import io.klira.franz.Job
import java.time.Instant
import java.util.concurrent.Delayed
import java.util.concurrent.TimeUnit

internal data class JobRetryInfo(val job: Job, val retryNo: Int, val retryTs: Long) : Delayed {
    companion object {
        private val DELAYS = uintArrayOf(0u, 20u, 30u, 100u, 600u, 3000u)
    }

    constructor(job: Job, retryNo: Int) : this(job, retryNo, Instant.now().epochSecond)

    internal fun nextAttempt() = JobRetryInfo(job, retryNo + 1)

    private fun currentDelay() = DELAYS[retryNo.toInt()].toULong()

    override fun getDelay(unit: TimeUnit?): Long =
            unit!!.convert(retryTs + currentDelay().toLong(), TimeUnit.SECONDS)

    override fun compareTo(other: Delayed?): Int =
            getDelay(TimeUnit.SECONDS).compareTo(other!!.getDelay(TimeUnit.SECONDS))
}