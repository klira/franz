plugins {
    base
    kotlin("jvm") version "1.3.20" apply false
}

allprojects {
    group = "io.klira"

    version = "0.2.0"

    repositories {
        jcenter()
        mavenCentral()
        gradlePluginPortal()
        maven("http://dl.bintray.com/kotlin/kotlin-eap")
    }
}



dependencies {
    // Make the root project archives configuration depend on every subproject
    subprojects.forEach {
        archives(it)
    }
}