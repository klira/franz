package io.klira.franz

interface Worker {
    suspend fun processMessage(job: Job) : JobUpdate
}

