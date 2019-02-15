package io.klira.franz.runtime

import io.klira.franz.Job
import io.klira.franz.Message

class BasicJob(private val message: Message) : Job {
    override fun message(): Message = message
}