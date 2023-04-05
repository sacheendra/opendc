plugins {
    `kotlin-conventions`
    id("io.ktor.plugin") version "2.2.4"
}

dependencies {
    implementation(libs.clikt)
    api(projects.opendcStorage.opendcDistributedCache)

    implementation(libs.commons.collections4)

    implementation("io.ktor:ktor-client-core:2.2.4")
    implementation("io.ktor:ktor-client-cio:2.2.4")
}

task("fatJar", type = Jar::class) {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest {
        attributes["Implementation-Title"] = "OpenDC remote invoker"
        // Needs the Kt at the end of classname to work
        // https://kotlinlang.org/docs/java-to-kotlin-interop.html#package-level-functions
        attributes["Main-Class"] = "org.opendc.storage.invoker.InvokerKt"
    }
    val dependencies = configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) }
    from(dependencies)
    with(tasks.jar.get())
}
