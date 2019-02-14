package io.klira.franz

interface Message {
    fun offset(): Long
    fun value(): ByteArray
    fun headers(): Array<Pair<String, ByteArray>>
    fun headers(key: String): Array<ByteArray>
    fun key(): ByteArray
    fun topic(): String
    fun timestamp(): Long
    /// A numerical id identicating which if any partition a message belongs to.
    fun partition(): Long
}