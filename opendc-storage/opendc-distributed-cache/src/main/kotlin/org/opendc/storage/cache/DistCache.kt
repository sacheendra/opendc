package org.opendc.storage.cache


import ch.supsi.dti.isin.consistenthash.ConsistentHash
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.opendc.simulator.kotlin.runSimulation
import org.opendc.trace.util.parquet.LocalParquetReader
import org.opendc.trace.util.parquet.LocalParquetWriter
import java.nio.file.Paths
import kotlin.IllegalArgumentException
import kotlin.time.Duration.Companion.seconds

fun main(args: Array<String>) = DistCache().main(args)

class DistCache : CliktCommand() {
    val inputFile: String by argument(help="Input trace file path")
    val outputFile: String by argument(help="Output result file path")
    val placementAlgo: String by argument(help="Object placement algorithm")
    // Autoscaler options
    val autoscalerEnabled: Boolean by option().flag(default=false)
    // Work stealing options
    val workstealEnabled: Boolean by option().flag(default=false)
    // Indirection based load balancing options
    // Indirection based autoscaling options
    // Prefetching options

    override fun run() {
        val start = System.currentTimeMillis()
        runSimulation {

            // Setup remote storage
            val rs = RemoteStorage()

            // Setup metrics recorder
            val resultWritePath = Paths.get(outputFile)
            val resultWriter = LocalParquetWriter.builder(resultWritePath, CacheTaskWriteSupport())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()
            val metricRecorder = MetricRecorder(60.seconds)

            // Setup scheduler
            val objectPlacer = mapPlacementAlgoName(placementAlgo)
            val scheduler = TaskScheduler(workstealEnabled, objectPlacer)

            // Setup hosts
            val numHosts = 11
            val addHostsFlow = flow {
                scheduler.addHosts((1..numHosts)
                    .map { CacheHost(4, 100, timeSource, rs, scheduler) })
                emit(Unit)
            }

            // Setup autoscaler
            val autoscaler = Autoscaler(60.seconds, timeSource, rs, scheduler, metricRecorder)

            // Write results for completed tasks
            val writeTaskFlow = scheduler.completedTaskFlow
                .onEach {
                    metricRecorder.recordCompletion(it)
                    resultWriter.write(it)
                }
                .onCompletion {
                    resultWriter.close()
                }

            // Load trace
            val traceReadPath = Paths.get(inputFile)
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
                        delay(maxOf(it.submitTime - currentTime, 1))
                        metricRecorder.recordSubmission(it)
                        scheduler.offerTask(it)
                    }
                }
                .onCompletion {
                    delay(lastTask!!.submitTime - currentTime + 1000)

                    scheduler.complete()
                    metricRecorder.complete()
                }


            // Start execution
            // No simulation runs till we call this
            val allFlows = mutableListOf(addHostsFlow, inputFlow, writeTaskFlow,
                metricRecorder.metricsFlow)
            if (autoscalerEnabled)
                metricRecorder.addCallback{ autoscaler.autoscale() }

            allFlows.merge().collect()
        }
        val end = System.currentTimeMillis()
        println((end - start) / 1000.0)
    }

    fun mapPlacementAlgoName(name: String): ConsistentHash {
        val algo: ConsistentHash.Algorithm = when(name) {
            "ring" -> ConsistentHash.Algorithm.RING_HASH
            "rendezvous" -> ConsistentHash.Algorithm.RENDEZVOUS_HASH
            "maglev" -> ConsistentHash.Algorithm.MAGLEV_HASH
            "jump" -> ConsistentHash.Algorithm.JUMP_HASH
            "dx" -> ConsistentHash.Algorithm.DX_HASH
            "anchor" -> ConsistentHash.Algorithm.ANCHOR_HASH
            // Beamer is missing
            else -> {
                throw IllegalArgumentException("Unknown placement algo name: ${name}")
            }
        }

        return ConsistentHash.create(algo, ConsistentHash.DEFAULT_HASH_ALGOTITHM)
    }
}
