// Common configuration for both `dbfy-jvm` (Java) and `dbfy-kotlin`
// (Kotlin extensions). Both publish to Maven Central under the
// `com.dbfy` group; signing + publication settings live here so they
// stay in sync.

// Core Gradle plugins (java-library, maven-publish, signing) live on
// the build classpath and don't need to be declared here when only
// the subprojects apply them. This root build.gradle.kts only carries
// shared configuration.

allprojects {
    group = "com.dbfy"
    version = providers.gradleProperty("dbfy.version").getOrElse("0.3.0")

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    configure<PublishingExtension> {
        repositories {
            maven {
                name = "sonatype"
                url = uri("https://central.sonatype.com/api/v1/publisher/")
                credentials {
                    username = providers.gradleProperty("sonatype.username")
                        .orElse(providers.environmentVariable("SONATYPE_USERNAME"))
                        .getOrElse("")
                    password = providers.gradleProperty("sonatype.token")
                        .orElse(providers.environmentVariable("SONATYPE_TOKEN"))
                        .getOrElse("")
                }
            }
        }
    }
}
