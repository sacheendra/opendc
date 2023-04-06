package org.opendc.storage.remote

import ch.supsi.dti.isin.consistenthash.ConsistentHash
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.opendc.storage.cache.CacheTaskWriteSupport
import org.opendc.storage.cache.DistCache
import org.opendc.storage.cache.TaskScheduler
import org.opendc.storage.cache.schedulers.CentralizedDataAwarePlacer
import org.opendc.storage.cache.schedulers.ConsistentHashWrapper
import org.opendc.storage.cache.schedulers.DelegatedDataAwarePlacer
import org.opendc.storage.cache.schedulers.GreedyObjectPlacer
import org.opendc.storage.cache.schedulers.ObjectPlacer
import org.opendc.storage.cache.schedulers.RandomObjectPlacer
import org.opendc.trace.util.parquet.LocalParquetWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Clock
import java.time.InstantSource
import kotlin.random.Random
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

fun main(args: Array<String>) = RemoteSched().main(args)

fun Application.module() {
    configureRouting()
}

class RemoteSched : CliktCommand() {
//    val outputFolder: String by argument(help="Output result file path")
    val placementAlgo: String by argument(help="Object placement algorithm")
    // Work stealing options
    val workstealEnabled: Boolean by option().flag(default=false)

    val rng = Random(42)

    override fun run() {

        // Setup scheduler
        val objectPlacer = mapPlacementAlgoName(placementAlgo, Clock.systemUTC())
        val scheduler = TaskScheduler(objectPlacer, false)
        GlobalScheduler.initialize(scheduler)

        embeddedServer(Netty, port = 19001, host = "0.0.0.0", module = Application::module)
            .start(wait = true)

    }

    fun mapPlacementAlgoName(name: String, timeSource: InstantSource): ObjectPlacer {
        if (name == "greedy") {
            return GreedyObjectPlacer()
        } else if (name == "random") {
            return RandomObjectPlacer()
        } else if (name == "centralized") {
            return CentralizedDataAwarePlacer(0.seconds, timeSource, false, workstealEnabled, false, false)
        } else if (name == "delegated") {
            val subPlacers = List(5) { _ -> CentralizedDataAwarePlacer(0.seconds, timeSource, false, workstealEnabled, false, false) }
            return DelegatedDataAwarePlacer(0.seconds, timeSource, subPlacers, false, rng=rng)
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

        return ConsistentHashWrapper(ConsistentHash.create(algo, ConsistentHash.DEFAULT_HASH_ALGOTITHM, 400), workstealEnabled, rng=rng)
    }
}
