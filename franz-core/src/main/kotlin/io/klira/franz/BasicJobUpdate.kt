package io.klira.franz

class BasicJobUpdate(private val b: Boolean) : JobUpdate {
    override fun mayAdvanceOffset(): Boolean = b
}