package io.klira.franz

class BasicJob(private val message: Message) : Job {
    override fun message(): Message = message
}