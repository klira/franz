package io.klira.franz

interface JobBatch {
    fun asSingle(): Job = asLinear().let {
        if (it.size > 1) throw Exception("More than one element in list")
        else it.first()
    }

    fun asLinear(): List<Job>
    fun asTagged(): Map<Any, List<Job>>
}

val JOB_BATCH_UNTAGGED = object {}

internal object EmptyJobBatch : JobBatch {
    override fun asLinear(): List<Job> = emptyList()
    override fun asTagged(): Map<Any, List<Job>> = emptyMap()
}

internal class ListJobBatch internal constructor(private val jobs: List<Job>) : JobBatch {
    override fun asLinear(): List<Job> = jobs
    override fun asTagged(): Map<Any, List<Job>> = mapOf(JOB_BATCH_UNTAGGED to jobs)
    override fun equals(other: Any?): Boolean =
            if (other is ListJobBatch)
                other.jobs == jobs
            else false
}

internal class TaggedJobBatch internal constructor(private val jobs: Map<Any, List<Job>>) : JobBatch {
    override fun asLinear() = jobs.flatMap { (_, v) -> v }
    override fun asTagged(): Map<Any, List<Job>> = jobs
}


object JobBatches {
    fun empty(): JobBatch = EmptyJobBatch
    fun fromSingleton(job: Job): JobBatch = ListJobBatch(listOf(job))
    fun fromList(jobs: List<Job>): JobBatch = ListJobBatch(jobs)
    fun fromTagged(jobs: Map<Any, List<Job>>): JobBatch = TaggedJobBatch(jobs)
    fun toSingle(jb: JobBatch): List<JobBatch> = jb.asLinear().map { fromSingleton(it) }
}