package org.opendc.storage.cache

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types

data class CacheTask(
    val taskId: Long,
    val objectId: Long,
    val duration: Long,
    val submitTime: Long,
    var startTime: Long,
    var endTime: Long,
    var isHit: Boolean = true,
    var hostId: Int,
    var storageDelay: Long,
)

class CacheTaskWriteSupport : WriteSupport<CacheTask>() {
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

    override fun write(record: CacheTask) {
        write(recordConsumer, record)
    }

    private fun write(consumer: RecordConsumer, record: CacheTask) {
        consumer.startMessage()

        consumer.startField("taskId", 0)
        consumer.addLong(record.taskId)
        consumer.endField("taskId", 0)

        consumer.startField("objectId", 1)
        consumer.addLong(record.objectId)
        consumer.endField("objectId", 1)

        consumer.startField("duration", 2)
        consumer.addLong(record.duration)
        consumer.endField("duration", 2)

        consumer.startField("submitTime", 3)
        consumer.addLong(record.submitTime)
        consumer.endField("submitTime", 3)

        consumer.startField("startTime", 4)
        consumer.addLong(record.startTime)
        consumer.endField("startTime", 4)

        consumer.startField("endTime", 5)
        consumer.addLong(record.endTime)
        consumer.endField("endTime", 5)

        consumer.startField("isHit", 6)
        consumer.addBoolean(record.isHit)
        consumer.endField("isHit", 6)

        consumer.startField("hostId", 7)
        consumer.addInteger(record.hostId)
        consumer.endField("hostId", 7)

        consumer.startField("storageDelay", 8)
        consumer.addLong(record.storageDelay)
        consumer.endField("storageDelay", 8)

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
                    .named("taskId"),
                Types
                    .required(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("objectId"),
                Types
                    .required(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("duration"),
                Types
                    .required(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("submitTime"),
                Types
                    .required(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("startTime"),
                Types
                    .required(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("endTime"),
                Types
                    .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                    .named("isHit"),
                Types
                    .required(PrimitiveType.PrimitiveTypeName.INT32)
                    .named("hostId"),
                Types
                    .required(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("storageDelay")
            )
            .named("task")
    }
}

