package org.opendc.storage.cache


import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.cancellable
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.launch
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.opendc.simulator.kotlin.runSimulation
import org.opendc.trace.util.parquet.LocalParquetWriter
import java.nio.file.Paths

@OptIn(FlowPreview::class)
fun main(args: Array<String>) {
    runSimulation {
        // Setup remote storage
        val rs = RemoteStorage()

        // Setup metrics recorder
        val p = Paths.get("storagesimout/test.gz.parquet")
        val w = LocalParquetWriter.builder(p, CacheTaskWriteSupport())
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()

        w.write(CacheTask(1,1,1,1,1,1,true,1,1))

        // Setup scheduler
        val scheduler = TaskScheduler()

        // Setup hosts
        val numHosts = 10
        for (i in 0 until numHosts) {
            val host = CacheHost(4, 100, timeSource, rs, scheduler)
            scheduler.addHost(host)
        }
        val completedTaskFlow = scheduler.hosts.map { it.completedTaskFlow }.asFlow()
            .flattenMerge(1000)

        val writeTask = launch(Dispatchers.IO) {
            completedTaskFlow.cancellable().collect {
                w.write(it)
            }
        }

        // Execute trace


        scheduler.complete()
        w.close()
    }
}
