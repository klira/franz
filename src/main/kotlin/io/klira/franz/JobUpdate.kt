package io.klira.franz

interface JobUpdate {
    fun mayAdvanceOffset() : Boolean
}