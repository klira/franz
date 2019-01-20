package io.klira.franz.engine

import io.klira.franz.JobUpdate
import io.klira.franz.impl.BasicJob

interface ConsumerPlugin {
    fun onPluginLoaded(c: Consumer) {}
    fun beforeStarting(c: Consumer) {}

    /// Called before consumer plugin
    fun beforeClosing() {}
    /// Handle jobstatuses
    fun handleJobUpdates(results: List<Pair<BasicJob, JobUpdate>>) {}

    fun onTick() {}


    fun produceJobs(): List<BasicJob> = emptyList<BasicJob>()


    fun onClose() {}

}