package io.klira.franz.examples

import io.klira.franz.Message
import io.klira.franz.worker.simple.SimpleWorker

class ExampleSW : SimpleWorker() {
    override suspend fun performWork(message: Message) {
        println("Got message of ${message.value().size} bytes")
    }
}

fun main(args: Array<String>) {
    io.klira.franz.runtime.Runtime.boot()
}