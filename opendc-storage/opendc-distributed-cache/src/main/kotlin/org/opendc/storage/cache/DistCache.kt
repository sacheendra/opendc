package org.opendc.storage.cache


import ch.supsi.dti.isin.consistenthash.ConsistentHash
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.pair
import com.github.ajalt.clikt.parameters.options.triple
import com.github.ajalt.clikt.parameters.types.double
import com.github.ajalt.clikt.parameters.types.long

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.opendc.simulator.kotlin.runSimulation
import org.opendc.storage.cache.schedulers.CentralizedDataAwarePlacer
import org.opendc.storage.cache.schedulers.ConsistentHashWrapper
import org.opendc.storage.cache.schedulers.DelegatedDataAwarePlacer
import org.opendc.storage.cache.schedulers.GreedyObjectPlacer
import org.opendc.storage.cache.schedulers.ObjectPlacer
import org.opendc.storage.cache.schedulers.RandomObjectPlacer
import org.opendc.trace.util.parquet.LocalParquetReader
import org.opendc.trace.util.parquet.LocalParquetWriter
import java.nio.file.Paths
import kotlin.IllegalArgumentException
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

fun main(args: Array<String>) = DistCache().main(args)

class DistCache : CliktCommand() {
    val inputFile: String by argument(help="Input trace file path")
    val outputFile: String by argument(help="Output result file path")
    val placementAlgo: String by argument(help="Object placement algorithm")
    // Autoscaler options
    val autoscalerEnabled: Boolean by option().flag(default=false)
    val watermarks: Pair<Double, Double> by option().double().pair().default(Pair(0.6, 0.9))
    val manualscalerEnabled: Boolean by option().flag(default=false)
    val manualOptions: Triple<Long, Long, Long> by option().long().triple().default(Triple(4000000, 11, 22))
    val builtinscalerEnabled: Boolean by option().flag(default=false)
    // Work stealing options
    val workstealEnabled: Boolean by option().flag(default=false)
    // Minimize movement for centralized algos
    val minMovement: Boolean by option().flag(default=true)
    // Indirection based load balancing options
    // Indirection based autoscaling options
    // Prefetching options

    override fun run() {

        val start = System.currentTimeMillis()
        runSimulation {

            // Setup remote storage
            val remoteStorage = RemoteStorage()

            // Setup metrics recorder
            val resultWritePath = Paths.get(outputFile)
            val resultWriter = LocalParquetWriter.builder(resultWritePath, CacheTaskWriteSupport())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()

            val scalingPeriod = 1.minutes
            val metricRecorder = MetricRecorder(scalingPeriod)

            val warmupDelay = 1000*100000

            val numHosts = if (manualscalerEnabled) {
                manualOptions.second.toInt()
            } else {
                11
            }
            // Setup scheduler
            val objectPlacer = mapPlacementAlgoName(placementAlgo, numHosts*10)
            val scheduler = TaskScheduler(objectPlacer)

            // Setup hosts
            val addHostsFlow = flow {
                scheduler.addHosts((1..numHosts)
                    .map { CacheHost(4, 100, timeSource, remoteStorage, scheduler, metricRecorder) })
                emit(Unit)
            }

            // Setup autoscaler
            val autoscaler = Autoscaler(timeSource, remoteStorage, scheduler, metricRecorder, watermarks)
            val manualScaler = ManualScaler(manualOptions.first + warmupDelay, manualOptions.third, scheduler, timeSource, remoteStorage, metricRecorder)

            // Write results for completed tasks
            val writeTaskFlow = scheduler.completedTaskFlow
                .onEach {
                    if (it.startTime > warmupDelay) {
                        it.startTime = it.startTime - warmupDelay
                        it.endTime = it.endTime - warmupDelay
                        resultWriter.write(it)
                    }
                }
                .onCompletion {
                    resultWriter.close()
                }

            // Load trace
            val traceReadPath = Paths.get(inputFile)
            val warmupReader = LocalParquetReader(traceReadPath, CacheTaskReadSupport(), false)
            val traceReader = LocalParquetReader(traceReadPath, CacheTaskReadSupport(), false)

            val warmupFlow = flow {
                while (true) {
                    val task = warmupReader.read()
                    if (task != null) emit(task)
                    else break
                }
            }
                .onEach {
                    launch {
                        delay(maxOf(it.submitTime - currentTime, 1))
                        metricRecorder.recordSubmission(it)
                        scheduler.offerTask(it)
                    }
                }.take(8000*120)

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
                        delay(maxOf(it.submitTime - currentTime, 1) + warmupDelay)
                        metricRecorder.recordSubmission(it)
                        scheduler.offerTask(it)
                    }
                }
                .onCompletion {
                    delay(lastTask!!.submitTime - currentTime + 1000 + warmupDelay)

                    // Need to stop timed events like autoscaling before the scheduler
                    metricRecorder.complete()
                    scheduler.complete()
                }


            // Start execution
            // No simulation runs till we call this
            val allFlows = mutableListOf(addHostsFlow, warmupFlow, inputFlow, writeTaskFlow,
                metricRecorder.metricsFlow)
            if (autoscalerEnabled)
                metricRecorder.addCallback{ autoscaler.autoscale() }
            else if (manualscalerEnabled)
                allFlows.add(manualScaler.manualFlow)
            else if (builtinscalerEnabled)
                objectPlacer.registerAutoscaler(autoscaler)

            allFlows.merge().collect()
        }
        val end = System.currentTimeMillis()
        println((end - start) / 1000.0)
        println("OK!")
    }

    fun mapPlacementAlgoName(name: String, size: Int): ObjectPlacer {
        if (name == "greedy") {
            return GreedyObjectPlacer()
        } else if (name == "random") {
            return RandomObjectPlacer()
        } else if (name == "centralized") {
            return CentralizedDataAwarePlacer(1.seconds, minMovement)
        } else if (name == "delegated") {
            return DelegatedDataAwarePlacer(10.seconds, 5, minMovement)
        }

        // Beamer is missing
        // Maybe other centralized stuff

        val algo: ConsistentHash.Algorithm = when(name) {
            "ring" -> ConsistentHash.Algorithm.RING_HASH
            "rendezvous" -> ConsistentHash.Algorithm.RENDEZVOUS_HASH
            "maglev" -> ConsistentHash.Algorithm.MAGLEV_HASH
//            "jump" -> ConsistentHash.Algorithm.JUMP_HASH
            "multiprobe" -> ConsistentHash.Algorithm.MULTIPROBE_HASH
            "dx" -> ConsistentHash.Algorithm.DX_HASH
            "anchor" -> ConsistentHash.Algorithm.ANCHOR_HASH
            else -> {
                throw IllegalArgumentException("Unknown placement algo name: ${name}")
            }
        }

        return ConsistentHashWrapper(ConsistentHash.create(algo, ConsistentHash.DEFAULT_HASH_ALGOTITHM, size), workstealEnabled)
    }
}
