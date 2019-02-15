package io.klira.franz

interface Consumer : Runnable {
    fun stopGracefully()
    fun setPluginMeta(key: String, value: Any)
    fun getPluginMeta(key: String): Any?
}