package io.klira.franz.runtime

import io.klira.franz.*
import io.klira.franz.engine.ConsumerPlugin
import io.klira.franz.engine.ConsumerPluginConfigurationError
import io.klira.franz.engine.ConsumerPluginLoadStatus
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.util.concurrent.atomic.AtomicBoolean

class ConsumerImpl(private val worker: Worker, private val plugins: List<ConsumerPlugin>) : Consumer, Runnable {

    companion object {
        @JvmStatic
        private val logger = KotlinLogging.logger {}
    }

    private val shouldRun = AtomicBoolean(true)

    private val pluginMeta = mutableMapOf<String, Any>()

    override fun setPluginMeta(key: String, value: Any) {
        pluginMeta[key] = value
    }

    override fun getPluginMeta(key: String): Any? = pluginMeta[key]


    inline fun preventSpinWait(timeTaken: Long, fn: () -> Unit) {
        val timerStart = System.nanoTime() / 1000
        val timerEnd = System.nanoTime() / 1000
        fn()
        if (timerEnd - timerStart < timeTaken) {
            Thread.sleep(timeTaken)
        }
    }

    override fun run() {
        val configErrors = runInAllPlugins { onPluginLoaded(this@ConsumerImpl) }
                .mapNotNull { it as? ConsumerPluginLoadStatus.ConfigurationError }

        if (configErrors.isNotEmpty()) {
            val errorMsg = configErrors.map { it.reason }.joinToString("\n")
            logger.error {
                "Configuration error in ${configErrors.size} plugins \n ${errorMsg}"
            }
            throw ConsumerPluginConfigurationError(configErrors.toList())
        }

        try {
            try {
                runInAllPlugins { beforeStarting(this@ConsumerImpl) }
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

    override fun stopGracefully() {
        this.shouldRun.lazySet(false)
    }

    private fun runJob(job: Job): Pair<Job, JobUpdate> {
        return runBlocking {
            runCatching {
                worker.processMessage(job)
            }.recover { BasicJobUpdate(false) }
                    .map { job to it }
                    .getOrThrow()
        }
    }
}
