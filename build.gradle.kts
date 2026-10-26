import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters

plugins {
    java
    id("com.google.protobuf") version "0.9.4" apply false
}

// Shared BuildService to serialize heavy Spark jobs if needed or bound concurrency
abstract class SparkMutexService : BuildService<BuildServiceParameters.None>

val sparkMutex = gradle.sharedServices.registerIfAbsent("sparkMutex", SparkMutexService::class.java) {
    maxParallelUsages.set(1)
}

subprojects {
    apply(plugin = "java")

    group = "com.featureforge"
    version = "1.0.0"

    repositories {
        mavenCentral()
    }

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        maxParallelForks = 1
        jvmArgs(
            "-XX:+ExitOnOutOfMemoryError",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.net=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
        )
    }
}