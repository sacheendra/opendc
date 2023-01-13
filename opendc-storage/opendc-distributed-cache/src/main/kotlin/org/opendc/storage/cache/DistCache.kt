package org.opendc.storage.cache


import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.cancellable
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.last
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.opendc.common.asCoroutineDispatcher
import org.opendc.simulator.kotlin.runSimulation
import org.opendc.trace.util.parquet.LocalParquetReader
import org.opendc.trace.util.parquet.LocalParquetWriter
import java.nio.file.Paths
import kotlin.time.Duration.Companion.seconds

@OptIn(FlowPreview::class)
fun main(args: Array<String>) {
    val start = System.currentTimeMillis()
    runSimulation {

        // Setup remote storage
        val rs = RemoteStorage()

        // Setup metrics recorder
        val resultWritePath = Paths.get("storagesimout/test_results_40static_faststeal.parquet")
        val resultWriter = LocalParquetWriter.builder(resultWritePath, CacheTaskWriteSupport())
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()

        // Setup scheduler
        val scheduler = TaskScheduler()

        // Setup hosts
        val numHosts = 40
        val addHostsFlow = (fun() {
            (1..numHosts)
                .map { CacheHost(4, 100, timeSource, rs, scheduler) }
                .onEach {
                    launch {
                        scheduler.addHost(it)
                    }
                }
        }).asFlow()

        // Setup autoscaler
//        val autoscaler = Autoscaler(60.seconds, timeSource, rs, scheduler)

        // Write results for completed tasks
        val writeTaskFlow = scheduler.completedTaskFlow
            .onEach {
//                autoscaler.recordCompletion(it)
                resultWriter.write(it)
            }
            .onCompletion {
                resultWriter.close()
            }

        // Load trace
        val traceReadPath = Paths.get("storagesimout/test_trace.parquet")
        val traceReader = LocalParquetReader(traceReadPath, CacheTaskReadSupport(), false)

        var lastTask: CacheTask? = null
        val inputFlow = flow {
            while (true) {
                val task = traceReader.read()
                if (task != null) emit(task)
                else break
            }
        }
            .onEach {
                lastTask = it
                launch {
                    delay(maxOf( it.submitTime - currentTime, 1))
//                    autoscaler.recordSubmission(it)
                    scheduler.offerTask(it)
                }
            }
            .onCompletion {
                delay(lastTask!!.submitTime - currentTime + 1000)

                scheduler.complete()
//                autoscaler.complete()
            }



        // Start execution
        // No simulation runs till we call this
        listOf(addHostsFlow, inputFlow, writeTaskFlow).merge().collect()
    }
    val end = System.currentTimeMillis()
    println((end - start) / 1000.0)
}
