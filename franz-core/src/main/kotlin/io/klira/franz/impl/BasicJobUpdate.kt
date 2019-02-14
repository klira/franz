package io.klira.franz.impl

import io.klira.franz.JobUpdate

class BasicJobUpdate(private val b: Boolean) : JobUpdate {
    override fun mayAdvanceOffset(): Boolean = b
}