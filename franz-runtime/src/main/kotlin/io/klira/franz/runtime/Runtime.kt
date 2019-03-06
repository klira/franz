package io.klira.franz.runtime

import io.klira.franz.Worker
import io.klira.franz.engine.ConsumerPlugin
import io.klira.franz.engine.ConsumerPluginOptions
import mu.KotlinLogging
import org.yaml.snakeyaml.Yaml
import java.nio.file.FileSystems
import java.nio.file.Files

class Runtime(private val config: Map<String, Any>) {

    private sealed class CodeReference {
        abstract fun getClassRef(): Class<*>
        private class BundledClass(val name: String) : CodeReference() {
            override fun getClassRef(): Class<*> =
                    Class.forName(name)
        }

        companion object {
            fun fromMap(m: Map<String, Any>): CodeReference = when {
                "className" in m -> BundledClass(m["className"] as String)
                else -> throw UnsupportedOperationException()
            }
        }
    }

    private data class PluginEntry private constructor(val codeRef: CodeReference, val options: ConsumerPluginOptions) {
        companion object {
            fun fromMap(m: Map<String, Any>) =
                    PluginEntry(
                            CodeReference.fromMap(m),
                            ConsumerPluginOptions((m["options"] as? Map<String, Any>) ?: emptyMap()))
        }
    }

    private data class SupervisorTaskConfig(private val worker: CodeReference, private val plugins: List<PluginEntry>) {
        companion object {
            fun fromMap(data: Map<String, Any>): SupervisorTaskConfig {
                val worker = CodeReference.fromMap(data["worker"] as Map<String, Any>)
                val plugins = (data["plugins"] as? List<Map<String, Any>> ?: emptyList())
                        .map { PluginEntry.fromMap(it) }
                return SupervisorTaskConfig(worker, plugins)
            }
        }

        fun addtoSupervisor(s: Supervisor) {
            val pluginInstances = plugins.asSequence()
                    .map {
                        it.codeRef
                                .getClassRef()
                                .getConstructor(ConsumerPluginOptions::class.java)
                                .newInstance(it.options)
                    }
                    .map { it as ConsumerPlugin }

            s.createPrototype(worker.getClassRef() as Class<Worker>) { pluginInstances.toList() }
        }
    }

    private data class SupervisorConfig(private val tasks: List<SupervisorTaskConfig>) {
        companion object {
            fun fromMap(supervisor: Map<String, Any>): SupervisorConfig =
                    (supervisor.getOrDefault("tasks", emptyList()) as List<Any>).map {
                        it as Map<String, Any>
                    }.map {
                        SupervisorTaskConfig.fromMap(it)
                    }.let { SupervisorConfig(it) }
        }

        fun addToSupervisor(supervisor: Supervisor) {
            tasks.map { it.addtoSupervisor(supervisor) }
        }
    }

    fun run() {
        val runtime = config["runtime"] as? Map<String, Any>
        requireNotNull(runtime)
        val conf = SupervisorConfig.fromMap(runtime["supervisor"] as Map<String, Any>)
        val (s, th) = Supervisor.spawnInThread()
        conf.addToSupervisor(s)
        th.join()
    }

    companion object {
        @JvmStatic
        private val logger = KotlinLogging.logger {}

        fun fromString(s: String): Runtime {
            val y = Yaml()
            val data = when (val data = y.load<Any>(s)) {
                is Map<*, *> ->
                    @Suppress("UNCHECKED_CAST")
                    data as Map<String, Any>
                else -> throw Exception("Bad YAML")
            }
            return Runtime(data)
        }

        fun fromFile(path: String): Runtime {
            val y = Yaml()
            val p = FileSystems.getDefault().getPath(path)
            val data = when (val data = Files.newInputStream(p).use { y.load<Any>(it) }) {
                is Map<*, *> ->
                    @Suppress("UNCHECKED_CAST")
                    data as Map<String, Any>
                else -> throw Exception("Bad YAML")
            }
            return Runtime(data)
        }

        fun boot(): Runtime =
            Runtime.fromFile("franz.yaml").also { it.run() }
    }

}