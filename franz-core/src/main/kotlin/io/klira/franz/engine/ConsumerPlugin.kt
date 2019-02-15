package io.klira.franz.engine

import io.klira.franz.Consumer
import io.klira.franz.Job
import io.klira.franz.JobUpdate

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


    fun produceJobs(): List<Job> = emptyList()


    fun onClose() {}

}