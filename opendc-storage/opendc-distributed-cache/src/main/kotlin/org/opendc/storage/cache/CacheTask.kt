package org.opendc.storage.cache

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types
import java.time.Duration
import java.time.Instant

data class CacheTask(
    val taskId: Long,
    val objectId: Long,
    val duration: Long,
    val submitTime: Long,
    var stolen: Boolean = false,
    var startTime: Long = -1,
    var endTime: Long = -1,
    var isHit: Boolean = true,
    var hostId: Int = -1,
    var storageDelay: Long = -1,
    var callback: ((event: TaskEvent) -> Unit)? = null,
)

enum class TaskEvent {
    SCHEDULED_NOW, LATEBIND_TOMBSTONE
}

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

        consumer.startField("stolen", 4)
        consumer.addBoolean(record.stolen)
        consumer.endField("stolen", 4)

        consumer.startField("startTime", 5)
        consumer.addLong(record.startTime)
        consumer.endField("startTime", 5)

        consumer.startField("endTime", 6)
        consumer.addLong(record.endTime)
        consumer.endField("endTime", 6)

        consumer.startField("isHit", 7)
        consumer.addBoolean(record.isHit)
        consumer.endField("isHit", 7)

        consumer.startField("hostId", 8)
        consumer.addInteger(record.hostId)
        consumer.endField("hostId", 8)

        consumer.startField("storageDelay", 9)
        consumer.addLong(record.storageDelay)
        consumer.endField("storageDelay", 9)

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
                    .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                    .named("stolen"),
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

class CacheTaskReadSupport() : ReadSupport<CacheTask>() {

    override fun init(context: InitContext): ReadContext {

        return ReadContext(READ_SCHEMA)
    }

    override fun prepareForRead(
        configuration: Configuration,
        keyValueMetaData: Map<String, String>,
        fileSchema: MessageType,
        readContext: ReadContext
    ): RecordMaterializer<CacheTask> = CacheTaskRecordMaterializer(readContext.requestedSchema)

    companion object {

        @JvmStatic
        val READ_SCHEMA: MessageType = Types.buildMessage()
            .addFields(
                Types
                    .optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("taskId"),
                Types
                    .optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("objectId"),
                Types
                    .optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("duration"),
                Types
                    .optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .named("submitTime")
            )
            .named("task")
    }
}

class CacheTaskRecordMaterializer(schema: MessageType) : RecordMaterializer<CacheTask>() {
    /**
     * State of current record being read.
     */
    private var taskId = -1L
    private var objectId = -1L
    private var duration = -1L
    private var submitTime = -1L

    /**
     * Root converter for the record.
     */
    private val root = object : GroupConverter() {
        /**
         * The converters for the columns of the schema.
         */
        private val converters = schema.fields.map { type ->
            when (type.name) {
                "taskId" -> object : PrimitiveConverter() {
                    override fun addLong(value: Long) {
                        taskId = value
                    }
                }
                "objectId" -> object : PrimitiveConverter() {
                    override fun addLong(value: Long) {
                        objectId = value
                    }
                }
                "duration" -> object : PrimitiveConverter() {
                    override fun addLong(value: Long) {
                        duration = value
                    }
                }
                "submitTime" -> object : PrimitiveConverter() {
                    override fun addLong(value: Long) {
                        submitTime = value
                    }
                }
                else -> error("Unknown column $type")
            }
        }

        override fun start() {
            taskId = -1L
            objectId = -1L
            duration = -1L
            submitTime = -1L
        }

        override fun end() {}

        override fun getConverter(fieldIndex: Int): Converter = converters[fieldIndex]
    }

    override fun getCurrentRecord(): CacheTask = CacheTask(taskId, objectId, duration, submitTime)

    override fun getRootConverter(): GroupConverter = root
}

