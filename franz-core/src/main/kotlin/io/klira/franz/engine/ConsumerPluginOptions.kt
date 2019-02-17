package io.klira.franz.engine

data class ConsumerPluginOptions(val options: Map<String, Any>) {
    fun getMap(opt: String): Map<String, Any> = options[opt] as Map<String, Any> ?: emptyMap()
}