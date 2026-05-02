rootProject.name = "dbfy-jvm-multi"

include(":dbfy-jvm")
include(":dbfy-kotlin")

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

// Auto-download the JDK toolchain Gradle needs (here JDK 11 for
// Kotlin's `jvmToolchain(11)`) when it's not available locally.
// In CI the runner already has the right JDK, so this is a no-op
// there; on a developer box with only JDK 25 it transparently
// fetches an 11 build from Adoptium.
plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}
