import org.jetbrains.kotlin.ir.backend.js.compile

/*
 * Copyright (c) 2023 AtLarge Research
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

description = "Library for simulating distributed cache workloads"

plugins {
    `kotlin-conventions`
    kotlin("plugin.serialization") version "1.7.20"
}

dependencies {
    api(projects.opendcSimulator.opendcSimulatorFlow)
    implementation(projects.opendcSimulator.opendcSimulatorCore)
    implementation(projects.opendcTrace.opendcTraceParquet)

    implementation(libs.commons.math3)
    implementation(libs.commons.collections4)

    implementation(libs.clikt)

    implementation(files("libs/netpar-1.0.0-jar-with-dependencies.jar"))

    testImplementation(libs.slf4j.simple)
}

task("fatJar", type = Jar::class) {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest {
        attributes["Implementation-Title"] = "OpenDC distributed cache simulator"
        // Needs the Kt at the end of classname to work
        // https://kotlinlang.org/docs/java-to-kotlin-interop.html#package-level-functions
        attributes["Main-Class"] = "org.opendc.storage.cache.DistCacheKt"
    }
    val dependencies = configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) }
    from(dependencies)
    with(tasks.jar.get())
}
