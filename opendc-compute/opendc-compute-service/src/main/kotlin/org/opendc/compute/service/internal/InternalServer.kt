/*
 * Copyright (c) 2021 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.opendc.compute.service.internal

import mu.KotlinLogging
import org.opendc.compute.api.Server
import org.opendc.compute.api.ServerState
import org.opendc.compute.api.ServerWatcher
import org.opendc.compute.service.driver.Host
import java.time.Instant
import java.util.UUID

/**
 * Internal implementation of the [Server] interface.
 */
internal class InternalServer(
    private val service: ComputeServiceImpl,
    override val uid: UUID,
    override val name: String,
    override val flavor: InternalFlavor,
    override val image: InternalImage,
    override val labels: MutableMap<String, String>,
    override val meta: MutableMap<String, Any>
) : Server {
    /**
     * The logger instance of this server.
     */
    private val logger = KotlinLogging.logger {}

    /**
     * The watchers of this server object.
     */
    private val watchers = mutableListOf<ServerWatcher>()

    /**
     * The [Host] that has been assigned to host the server.
     */
    @JvmField internal var host: Host? = null

    /**
     * The most recent timestamp when the server entered a provisioning state.
     */
    override var launchedAt: Instant? = null

    /**
     * The current scheduling request.
     */
    private var request: ComputeServiceImpl.SchedulingRequest? = null

    override suspend fun start() {
        when (state) {
            ServerState.RUNNING -> {
                logger.debug { "User tried to start server but server is already running" }
                return
            }
            ServerState.PROVISIONING -> {
                logger.debug { "User tried to start server but request is already pending: doing nothing" }
                return
            }
            ServerState.DELETED -> {
                logger.warn { "User tried to start terminated server" }
                throw IllegalStateException("Server is terminated")
            }
            else -> {
                logger.info { "User requested to start server $uid" }
                state = ServerState.PROVISIONING
                assert(request == null) { "Scheduling request already active" }
                request = service.schedule(this)
            }
        }
    }

    override suspend fun stop() {
        when (state) {
            ServerState.PROVISIONING -> {
                cancelProvisioningRequest()
                state = ServerState.TERMINATED
            }
            ServerState.RUNNING, ServerState.ERROR -> {
                val host = checkNotNull(host) { "Server not running" }
                host.stop(this)
            }
            else -> {} // No work needed
        }
    }

    override suspend fun delete() {
        when (state) {
            ServerState.PROVISIONING, ServerState.TERMINATED -> {
                cancelProvisioningRequest()
                service.delete(this)
                state = ServerState.DELETED
            }
            ServerState.RUNNING, ServerState.ERROR -> {
                val host = checkNotNull(host) { "Server not running" }
                host.delete(this)
                service.delete(this)
                state = ServerState.DELETED
            }
            else -> {} // No work needed
        }
    }

    override fun watch(watcher: ServerWatcher) {
        watchers += watcher
    }

    override fun unwatch(watcher: ServerWatcher) {
        watchers -= watcher
    }

    override suspend fun refresh() {
        // No-op: this object is the source-of-truth
    }

    override var state: ServerState = ServerState.TERMINATED
        set(value) {
            if (value != field) {
                watchers.forEach { it.onStateChanged(this, value) }
            }

            field = value
        }

    /**
     * Cancel the provisioning request if active.
     */
    public fun cancelProvisioningRequest() {
        val request = request
        if (request != null) {
            this.request = null
            request.isCancelled = true
        }
    }

    override fun equals(other: Any?): Boolean = other is Server && uid == other.uid

    override fun hashCode(): Int = uid.hashCode()

    override fun toString(): String = "Server[uid=$uid,state=$state]"
}
