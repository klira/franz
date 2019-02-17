package io.klira.franz.engine.plugins.retry

import io.klira.franz.Job
import io.klira.franz.JobBatch
import io.klira.franz.JobBatches
import io.klira.franz.JobUpdate
import io.klira.franz.engine.ConsumerPlugin
import io.klira.franz.engine.ConsumerPluginOptions

import java.util.concurrent.DelayQueue
import java.util.concurrent.TimeUnit


class RetryManager(private val opts: ConsumerPluginOptions) : ConsumerPlugin {
    private val rescheduledJobs = DelayQueue<JobRetryInfo>()
    private val jobToRetryInfo = mutableMapOf<Job, JobRetryInfo>()
    private fun drainRetries(): List<Job> {
        val lst = mutableListOf<Job>()
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

    override fun produceJobs(): JobBatch {
        return JobBatches.fromList(drainRetries())
    }

    override fun handleJobUpdates(results: List<Pair<Job, JobUpdate>>) {
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