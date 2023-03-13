package org.opendc.storage.cache.schedulers

import kotlinx.coroutines.flow.Flow
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.CacheTask
import org.opendc.storage.cache.TaskScheduler

interface ObjectPlacer {

    fun addHosts(hosts: List<CacheHost>)

    fun removeHosts(hosts: List<CacheHost>)

    var scheduler: TaskScheduler
    fun registerScheduler(scheduler: TaskScheduler) {
        this.scheduler = scheduler
    }

    fun getPlacerFlow(): Flow<TimeCountPair>?

    suspend fun complete()

    suspend fun getNextTask(host: CacheHost): CacheTask?

    suspend fun offerTask(task: CacheTask)
}

data class TimeCountPair(
    var time: Long,
    val count: Int
)

class TimeCountWriteSupport : WriteSupport<TimeCountPair>() {
    /**
     * The current active record consumer.
     */
    private lateinit var recordConsumer: RecordConsumer

    override fun init(configuration: Configuration): WriteContext {
        return WriteContext(WRITE_SCHEMA, emptyMap())
    }

    override fun prepareForWrite(recordConsumer: RecordConsumer) {
        this.recordConsumer = recordConsumer
    }

    override fun write(record: TimeCountPair) {
        write(recordConsumer, record)
    }

    private fun write(consumer: RecordConsumer, record: TimeCountPair) {
        consumer.startMessage()

        consumer.startField("time", 0)
        consumer.addLong(record.time)
        consumer.endField("time", 0)

        consumer.startField("count", 1)
        consumer.addInteger(record.count)
        consumer.endField("count", 1)

        consumer.endMessage()
    }

    companion object {
        /**
         * Parquet schema for the "resources" table in the trace.
         */
        @JvmStatic
        val WRITE_SCHEMA: MessageType = Types.buildMessage()
            .addFields(
                Types
                    .required(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("time"),
                Types
                    .required(PrimitiveType.PrimitiveTypeName.INT32)
                    .named("count")
            )
            .named("timecount")
    }
}
