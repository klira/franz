package io.klira.franz.engine

import io.klira.franz.JobUpdate
import io.klira.franz.Worker
import io.klira.franz.impl.BasicJob
import io.klira.franz.impl.BasicJobUpdate
import kotlinx.coroutines.runBlocking
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

class Consumer(private val worker: Worker, private val plugins: List<ConsumerPlugin>) : Runnable {

    private val shouldRun = AtomicBoolean(true)

    private val pluginMeta = mutableMapOf<String, Any>()

    fun setPluginMeta(key: String, value: Any) {
        pluginMeta[key] = value
    }

    fun getPluginMeta(key: String) = pluginMeta[key]


    inline fun preventSpinWait(timeTaken: Long, fn: () -> Unit) {
        val timerStart = System.nanoTime() / 1000
        val timerEnd = System.nanoTime() / 1000
        fn()
        if (timerEnd - timerStart < timeTaken) {
            Thread.sleep(timeTaken)
        }
    }

    override fun run() {
        runInAllPlugins { onPluginLoaded(this@Consumer) }
        try {
            try {
                runInAllPlugins { beforeStarting(this@Consumer) }
                while (shouldRun.get()) {
                    preventSpinWait(500) {
                        runInAllPlugins {
                            val results = produceJobs().map { job -> runJob(job) }
                            runInAllPlugins {
                                handleJobUpdates(results)
                            }
                        }
                        runInAllPlugins { onTick() }
                    }
                }
            } finally {
                runInAllPluginsThrowFirst { beforeClosing() }
            }
        } finally {
            runInAllPluginsThrowFirst { onClose() }
        }
    }

    private inline fun <T> runInAllPlugins(fn: ConsumerPlugin.() -> T): List<T> =
            plugins.map { fn(it) }

    private inline fun <T> runInAllPluginsCatching(fn: ConsumerPlugin.() -> T): List<Result<T>> =
            runInAllPlugins { kotlin.runCatching { fn(this) } }

    private inline fun <T> runInAllPluginsThrowFirst(fn: ConsumerPlugin.() -> T) =
            runInAllPluginsCatching(fn).forEach {
                it.getOrThrow()
            }

    fun stopGracefully() {
        this.shouldRun.lazySet(false)
    }

    private fun runJob(job: BasicJob): Pair<BasicJob, JobUpdate> {
        return runBlocking {
            runCatching {
                worker.processMessage(job)
            }.recover { BasicJobUpdate(false) }
                    .map { job to it }
                    .getOrThrow()
        }
    }
}
