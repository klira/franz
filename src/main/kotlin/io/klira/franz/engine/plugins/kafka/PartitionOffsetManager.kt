package io.klira.franz.engine.plugins.kafka

import java.util.*

class PartitionOffsetManager {
    private var maxCompleted = 0L
    private val failingJobs = TreeSet<Long>()

    fun onFailed(offset: Long) {
        failingJobs.add(offset)
    }

    fun onSuccess(offset: Long) : Optional<Long> {
        // The logic for advancing offsets is really complicated.
        // There are four cases for it:

        // The trivial case is when there are no failures.
        // We then simply commit the new offset
        if (failingJobs.isEmpty()) {
            return Optional.of(offset + 1)
        }

        // But if there are failures, we must remember the highest offset we've reached
        // So we can move the offset here when all the failures have been addressed.
        if (offset > maxCompleted) {
            maxCompleted = offset
        }

        // We then consider if the job we removed was lowest-offset tracked failing job...
        val wasFirst = failingJobs.first() == offset
        val didRemove = failingJobs.remove(offset)
        val didRemoveFirst = didRemove && wasFirst


        return when {
            // ...If it wasn't, the situation hasn't changed and we can't move the offset.
            !didRemoveFirst -> Optional.empty()
            // If it was, however, we need to consider two possibilities:
            // It was the last failing job,
            // we can commit maximum value we have seen completed.
            failingJobs.isEmpty() -> Optional.of(maxCompleted + 1)
            // There are still messages blocking,
            // we commit the offset of this message
            else -> Optional.of(offset + 1)
        }
    }
}