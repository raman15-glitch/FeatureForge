plugins {
    java
}

dependencies {
    implementation(project(":proto"))
    implementation(project(":feature-lib"))
    implementation("org.apache.spark:spark-sql_2.13:3.5.1")
}

fun registerSparkStage(taskName: String, mainClassName: String, vararg stageArgs: String) {
    tasks.register<JavaExec>(taskName) {
        group = "pipeline"
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set(mainClassName)
        args(*stageArgs)
        maxHeapSize = "2g"
        usesService(rootProject.extra["sparkMutex"] as Provider<*>)
        jvmArgs(
            "-XX:+ExitOnOutOfMemoryError",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED"
        )
    }
}

registerSparkStage("labels", "com.featureforge.batch.LabelGenerationJob", "build/pipeline/events_all", "build/pipeline/labels")
registerSparkStage("features", "com.featureforge.batch.PointInTimeFeatureJob", "build/pipeline/events_accepted", "build/pipeline/features")