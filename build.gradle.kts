import java.net.URI

group = "io.klira"
version = "0.2.0"
description = "franz"

defaultTasks = listOf("test")

plugins {
    java
    `java-library`
    maven
    `kotlin-dsl`
    //id("org.jetbrains.dokka")
    //id("org.jetbrains.kotlin.jvm")
    idea
    wrapper
    kotlin("jvm") version "1.3.10"
}
val kotlinVersion = "1.3.10"
val jvmVersion = "1.8"

buildscript {
    repositories {
        jcenter()
        mavenCentral()
        maven("https://plugins.gradle.org/m2/")
        maven("http://dl.bintray.com/kotlin/kotlin-eap")
    }
}

repositories {
    mavenCentral()
    jcenter()
    mavenLocal()
    maven("https://jitpack.io")
}

dependencies {
    compile(kotlin("stdlib"))
    implementation("io.github.microutils:kotlin-logging:1.6.20")
    implementation(group="org.apache.kafka", name="kafka-clients", version="2.0.0")
    implementation("io.github.microutils:kotlin-logging:1.6.22")
    runtime("ch.qos.logback:logback-classic:1.2.3")
    testCompile("org.junit.jupiter:junit-jupiter-api:5.3.0")
    testRuntime ("org.junit.jupiter:junit-jupiter-engine:5.3.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.1.0")
    compile(group= "org.yaml", name = "snakeyaml", version = "1.23")
}