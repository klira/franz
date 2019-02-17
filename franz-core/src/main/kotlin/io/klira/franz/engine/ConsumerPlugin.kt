package io.klira.franz.engine

import io.klira.franz.*

interface ConsumerPlugin {
    fun onPluginLoaded(c: Consumer): ConsumerPluginLoadStatus {
        return ConsumerPluginLoadStatus.Success
    }

    fun beforeStarting(c: Consumer) {}

    /// Called before consumer plugin
    fun beforeClosing() {}

    /// Handle jobstatuses
    fun handleJobUpdates(results: List<Pair<Job, JobUpdate>>) {}

    fun onTick() {}


    fun produceJobs(): JobBatch = EmptyJobBatch


    fun onClose() {}

}