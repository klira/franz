package io.klira.franz.engine

sealed class ConsumerPluginLoadStatus {
    object Success : ConsumerPluginLoadStatus()
    data class ConfigurationError(val reason: String) : ConsumerPluginLoadStatus()
}