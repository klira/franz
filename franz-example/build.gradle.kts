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
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.1.0")
    compile(project(":franz-runtime"))
    compile(project(":franz-api-simple"))
}