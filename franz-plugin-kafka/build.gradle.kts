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
    implementation(group = "org.apache.kafka", name = "kafka-clients", version = "2.0.0")
    implementation("io.github.microutils:kotlin-logging:1.6.22")
    runtime("ch.qos.logback:logback-classic:1.2.3")
    testCompile("org.junit.jupiter:junit-jupiter-api:5.3.0")
    testRuntime("org.junit.jupiter:junit-jupiter-engine:5.3.0")
    compile(project(":franz-core"))
}