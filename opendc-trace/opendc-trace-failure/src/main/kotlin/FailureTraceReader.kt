
package org.opendc.trace.failure

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvParser
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import java.nio.file.Path
import kotlin.io.path.inputStream

public object FailureTraceReader {
    private val mapper = CsvMapper()
        .enable(CsvParser.Feature.ALLOW_COMMENTS)
        .enable(CsvParser.Feature.TRIM_SPACES)

    private val schema = CsvSchema.builder()
        .addNumberColumn("start")
        .addNumberColumn("duration")
        .addNumberColumn("intensity")
        .build()

    public fun get(filepath: Path): MutableList<FailureInstance> {
        return mapper.readerFor(FailureInstance::class.java)
            .with(schema.withSkipFirstDataRow(true))
            .readValues<FailureInstance>(filepath.inputStream())
            .readAll()
    }
}

public data class FailureInstance(
    @field:JsonProperty("start") val start: Long,
    @field:JsonProperty("duration") val duration: Long,
    @field:JsonProperty("intensity") val intensity: Double
) {
    public constructor() : this(0, 0,0.0)
}
