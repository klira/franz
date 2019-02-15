plugins {
    java
    `java-library`
    maven
    `kotlin-dsl`
    idea
    wrapper
}
val kotlinVersion = "1.3.10"
val jvmVersion = "1.8"

dependencies {
    compile(kotlin("stdlib"))
    implementation("io.github.microutils:kotlin-logging:1.6.20")
    runtime("ch.qos.logback:logback-classic:1.2.3")
    testCompile("org.junit.jupiter:junit-jupiter-api:5.3.0")
    testRuntime("org.junit.jupiter:junit-jupiter-engine:5.3.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.1.0")
    compile(group = "org.yaml", name = "snakeyaml", version = "1.23")
}