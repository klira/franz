package io.klira.franz.supervisor

import io.klira.franz.Worker
import io.klira.franz.engine.Consumer
import io.klira.franz.engine.ConsumerPlugin
import mu.KotlinLogging

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock


class Supervisor : Runnable {
    companion object {
        @JvmStatic
        private val logger = KotlinLogging.logger {}
        fun spawnInThread() : Pair<Supervisor, Thread> {
            val s = Supervisor()
            val th = Thread(s)
            th.start()
            return s to th
        }
    }
    private data class WorkerPrototype(val workerClass: Class<*>,
                                       val createPlugins: () -> List<ConsumerPlugin>,
                                       val nWorkers: Int = 1)
    private data class WorkerRuntimeInfo(val worker: Worker, val consumer: Consumer, val thread: Thread)
    private val dataLock = ReentrantLock()
    private var data = mutableMapOf<WorkerPrototype, List<WorkerRuntimeInfo>>()
    private val shouldRun = AtomicBoolean(true)
    private val shutdownHook = Thread {
        stopGracefully()
    }
    fun <T : Worker> createPrototype(workerClass: Class<T>, createPlugins: () -> List<ConsumerPlugin>) {
        dataLock.lock()
        try {
            val wp = WorkerPrototype(workerClass, createPlugins)
            data[wp] = emptyList()
        } finally {
            dataLock.unlock()
        }
    }

    private fun spawn(wp: WorkerPrototype) : WorkerRuntimeInfo {
        // The validity is guaranteed by the fact that adding prototypes
        // can only be done using a method taking subclassses of worker
        val worker = wp.workerClass.newInstance() as Worker
        val cons = Consumer(worker, wp.createPlugins())
        logger.info { "Spawning new worker for ${wp.workerClass.name}" }
        val th = Thread(cons)
        th.start()
        return WorkerRuntimeInfo(worker, cons, th)
    }

    override fun run() {
        logger.info { "Supervisor starting up" }
        try {
            Runtime.getRuntime().addShutdownHook(shutdownHook)
            while(shouldRun.get()) {
                try {
                    dataLock.lock()
                    update()
                } finally {
                    dataLock.unlock()
                }

                Thread.sleep(500)
            }
            logger.info { "Supervisor shutdown processing begining, unscheduling all workers" }
            data = data.mapKeys { (k, v) ->
                k.copy(nWorkers = 0)
            }.toMutableMap()
            update()
            data.forEach { k, v ->
                v.forEach { (_, _, th) ->
                    th.join()
                }
            }
        } finally {
            Runtime.getRuntime().removeShutdownHook(shutdownHook)
        }

    }

    fun stopGracefully() {
        this.shouldRun.lazySet(false)
    }

    private fun update() {
        val newData = data
                // PHASE 1: Find dead workers and remove them.
                .mapValues { (proto, actual) ->
                    actual.filter { (_, _, th) -> th.isAlive }.also {
                        val deadThreads = actual.size - it.size
                        if (deadThreads > 0) {
                            logger.info { "Found ${actual.size - it.size} dead threads" }
                        }
                    }
                }
                // PHASE 2: Find superfluous workers and gently ask them to remove themselves.
                .mapValues { (proto, actual) ->
                    actual.drop(proto.nWorkers)
                            .also {
                                if (it.isNotEmpty()) {
                                    logger.info { "Found ${it.size} superfluous workers" }
                                }
                            }
                            .forEach { (_worker, consumer, _th) ->
                                consumer.stopGracefully()
                            }
                    actual
                }
                // PHASE 3: Find prototypes that are missing workers and create those.
                .mapValues { (proto, actual) ->
                    val toSpawn = Math.min(proto.nWorkers - actual.size, 0)
                    val spawned = (0..toSpawn).map { spawn(proto) }
                    actual + spawned
                }
        data = newData.toMutableMap()
    }
}