package io.klira.franz.engine

class ConsumerPluginConfigurationError(reason: String) : Exception("Failed to configure:" + reason) {
    constructor(errors: List<ConsumerPluginLoadStatus.ConfigurationError>) : this(errors.joinToString("\n"))
}