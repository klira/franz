package io.klira.franz.engine.plugins.retry

import io.klira.franz.JobUpdate
import io.klira.franz.engine.ConsumerPlugin
import io.klira.franz.impl.BasicJob
import java.util.concurrent.DelayQueue
import java.util.concurrent.TimeUnit


class RetryManager : ConsumerPlugin {
    private val rescheduledJobs = DelayQueue<JobRetryInfo>()
    private val jobToRetryInfo = mutableMapOf<BasicJob, JobRetryInfo>()
    private fun drainRetries(): List<BasicJob> {
        val lst = mutableListOf<BasicJob>()
        while (lst.size < 11) {
            val p = rescheduledJobs.peek()
            if (p != null && p.getDelay(TimeUnit.SECONDS) <= 0L) {
                lst.add(rescheduledJobs.take().job)
            } else {
                break
            }
        }
        return lst.toList()
    }

    override fun produceJobs(): List<BasicJob> {
        return drainRetries()
    }

    override fun handleJobUpdates(results: List<Pair<BasicJob, JobUpdate>>) {
        results
                .map { (j, u) -> u.mayAdvanceOffset() to j }
                .forEach { (mayAdvance, j) ->
                    if (!mayAdvance) {
                        val retryInfo = jobToRetryInfo[j]?.nextAttempt() ?: JobRetryInfo(j, 1)
                        rescheduledJobs.add(retryInfo)
                        jobToRetryInfo[j] = retryInfo
                    } else {
                        val retryInfo = jobToRetryInfo.remove(j)
                        if (retryInfo != null) {
                            rescheduledJobs.remove(retryInfo)
                        }
                    }
                }
    }
}