val ktor_version: String by project
val kotlin_version: String by project
val logback_version: String by project

plugins {
    `kotlin-conventions`
    id("io.ktor.plugin") version "2.2.4"
}

dependencies {
    implementation(libs.clikt)
    implementation(projects.opendcStorage.opendcDistributedCache)
    implementation(projects.opendcTrace.opendcTraceParquet)

    implementation(files("../libs/netpar-1.0.0-jar-with-dependencies.jar"))

    implementation("io.ktor:ktor-server-core-jvm:$ktor_version")
    implementation("io.ktor:ktor-server-netty-jvm:$ktor_version")
    implementation("ch.qos.logback:logback-classic:$logback_version")
}

task("fatJar", type = Jar::class) {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest {
        attributes["Implementation-Title"] = "OpenDC remote scheduler"
        // Needs the Kt at the end of classname to work
        // https://kotlinlang.org/docs/java-to-kotlin-interop.html#package-level-functions
        attributes["Main-Class"] = "org.opendc.storage.remote.ApplicationKt"
    }
    val dependencies = configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) }
    from(dependencies)
    with(tasks.jar.get())
}
