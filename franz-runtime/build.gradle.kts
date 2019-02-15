plugins {
    java
    `java-library`
    maven
    `kotlin-dsl`
    idea
    wrapper
}

dependencies {
    compile(kotlin("stdlib"))
    implementation("io.github.microutils:kotlin-logging:1.6.22")
    runtime("ch.qos.logback:logback-classic:1.2.3")
    testCompile("org.junit.jupiter:junit-jupiter-api:5.3.0")
    testRuntime("org.junit.jupiter:junit-jupiter-engine:5.3.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.1.0")
    compile(project(":franz-core"))
    runtime(project(":franz-plugin-retry"))
    runtime(project(":franz-plugin-kafka"))
}