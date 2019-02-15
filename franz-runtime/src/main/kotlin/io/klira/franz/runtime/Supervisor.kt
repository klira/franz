package io.klira.franz.runtime

import io.klira.franz.Consumer
import io.klira.franz.Worker
import io.klira.franz.engine.ConsumerPlugin
import io.klira.franz.engine.ConsumerPluginConfigurationError
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock


class Supervisor : Runnable {
    companion object {
        @JvmStatic
        private val logger = KotlinLogging.logger {}

        fun spawnInThread(): Pair<Supervisor, Thread> {
            val s = Supervisor()
            val th = Thread(s)
            th.name = "Supervisor"
            th.start()
            return s to th
        }
    }

    private data class WorkerPrototype(val workerClass: Class<*>,
                                       val createPlugins: () -> List<ConsumerPlugin>,
                                       val nWorkers: Int = 1) {
        fun name(): String = workerClass.name
    }

    private data class WorkerRuntimeInfo(val worker: Worker, val consumer: Consumer, val thread: Thread)
    private data class WorkerTableEntry(val proto: WorkerPrototype, val entries: List<WorkerRuntimeInfo>, val crashes: Int = 0, val exits: Int = 0)

    private var workerId = 1
    private val dataLock = ReentrantLock()
    private var data = listOf<WorkerTableEntry>()
    private val shouldRun = AtomicBoolean(true)
    private val crashes = ConcurrentHashMap<Thread, Throwable>()
    private val shutdownHook = Thread {
        stopGracefully()
    }
    private val consumerExceptionHandler = Thread.UncaughtExceptionHandler { th, ex ->
        if (ex is ConsumerPluginConfigurationError) {
            logger.error(ex) { "ConsumerPlugin was misconfigured, stopping" }
            shouldRun.set(false)
        } else {
            crashes[th] = ex
        }
    }

    fun <T : Worker> createPrototype(workerClass: Class<T>, createPlugins: () -> List<ConsumerPlugin>) {
        dataLock.lock()
        try {
            data += WorkerTableEntry(
                    WorkerPrototype(workerClass, createPlugins),
                    emptyList()
            )
        } finally {
            dataLock.unlock()
        }
    }

    private fun spawn(wp: WorkerPrototype): WorkerRuntimeInfo {
        // The validity is guaranteed by the fact that adding prototypes
        // can only be done using a method taking subclassses of worker
        val worker = wp.workerClass.newInstance() as Worker
        val cons = ConsumerImpl(worker, wp.createPlugins())
        logger.info { "Spawning new worker for ${wp.name()}" }
        val th = Thread(cons)
        th.name = "Worker-${workerId++}"
        th.uncaughtExceptionHandler = consumerExceptionHandler
        th.start()
        return WorkerRuntimeInfo(worker, cons, th)
    }

    override fun run() {
        logger.info { "Supervisor starting up" }
        try {
            java.lang.Runtime.getRuntime().addShutdownHook(shutdownHook)
            while (shouldRun.get()) {
                try {
                    dataLock.lock()
                    update()
                } finally {
                    dataLock.unlock()
                }

                Thread.sleep(500)
            }
            logger.info { "Supervisor shutdown processing begining, unscheduling all workers" }
            data = data.map {
                it.copy(proto = it.proto.copy(nWorkers = 0))
            }.toMutableList()
            update()
            data.forEach { (k, v) ->
                v.forEach { (_, _, th) ->
                    th.join()
                }
            }
        } finally {
            java.lang.Runtime.getRuntime().removeShutdownHook(shutdownHook)
        }

    }

    fun stopGracefully() {
        this.shouldRun.lazySet(false)
    }

    private var crashingTicks = 0
    private fun update() {
        val stillRunning = shouldRun.get()
        val oldCrashes = data.sumBy { it.crashes }
        val newData = performUpdate(stillRunning)
        data = newData.toMutableList()
        if (stillRunning) {
            countNewCrashes(oldCrashes)
        }

    }

    private fun performUpdate(stillRunning: Boolean): List<WorkerTableEntry> = data
            // PHASE 1: Find dead workers and remove them.
            .map {
                cleanupDeadWorkers(it)
            }
            // PHASE 2: Find superfluous workers and gently ask them to remove themselves.
            .map {
                shutdownSuperfluousWorkers(it)
            }
            // PHASE 3: Find prototypes that are missing workers and create those.
            .map {
                if (stillRunning) {
                    spawnRequiredWorkers(it)
                } else {
                    it
                }
            }

    private fun countNewCrashes(oldCrashes: Int) {
        val newCrashes = data.sumBy { it.crashes }
        val crashDelta = (newCrashes - oldCrashes)
        if (crashDelta > 0) {
            crashingTicks += 1
        } else if (crashingTicks > 0) {
            // Reduce the counter when we have a crash free tick
            crashingTicks -= 1
        }

        if (crashingTicks > 5) {
            logger.error { "${crashingTicks} crashing ticks, shutting down thex supervisor" }
            stopGracefully()
        }
    }

    private fun spawnRequiredWorkers(it: WorkerTableEntry): WorkerTableEntry {
        val (proto, actual) = it
        val toSpawn = Math.min(proto.nWorkers - actual.size, 0)
        val spawned = (0..toSpawn).map { spawn(proto) }
        return it.copy(entries = actual + spawned)
    }

    private fun shutdownSuperfluousWorkers(it: WorkerTableEntry): WorkerTableEntry {
        val (proto, actual) = it
        proto to actual.drop(proto.nWorkers)
                .also {
                    if (it.isNotEmpty()) {
                        logger.info { "Found ${it.size} superfluous workers" }
                    }
                }
                .forEach { (_worker, consumer, _th) ->
                    consumer.stopGracefully()
                }
        return it
    }

    private fun cleanupDeadWorkers(it: WorkerTableEntry): WorkerTableEntry {
        val (proto, actual) = it
        val stillAlive = actual.filter { (_, _, th) -> th.isAlive }
        val died = actual.filterNot { (_, _, th) -> th.isAlive }
        val (newCrashes, newExits) = if (died.isNotEmpty()) {
            val exs = died.mapNotNull { (_, _, th) -> crashes.remove(th) }
            exs.forEach { ex ->
                logger.error(ex) { "Worker ${proto.name()} crashed" }
            }
            val exited = died.size - exs.size
            val exceptions = exs.size
            logger.info { "Found ${exceptions} crashed threads and ${exited} exited threads" }
            exceptions to exited
        } else 0 to 0
        return it.copy(entries = stillAlive,
                crashes = it.crashes + newCrashes,
                exits = it.exits + newExits)
    }


}