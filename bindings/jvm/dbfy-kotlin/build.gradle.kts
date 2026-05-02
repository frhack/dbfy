// `dbfy-kotlin` — Kotlin extensions over the `dbfy-jvm` JNI surface.
// Uses suspend functions for one-shot queries and a Channel-backed
// Flow for streaming, both wrappers around the underlying
// CompletableFuture-based Java API.

plugins {
    kotlin("jvm") version "2.2.0"
    `java-library`
    `maven-publish`
    signing
}

kotlin {
    jvmToolchain(11)
}

dependencies {
    api(project(":dbfy-jvm"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.10.1")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.1")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

publishing {
    publications {
        create<MavenPublication>("dbfyKotlin") {
            artifactId = "dbfy-kotlin"
            from(components["java"])
            pom {
                name.set("dbfy-kotlin")
                description.set(
                    "Kotlin coroutine extensions over the dbfy-jvm JNI bindings: " +
                    "suspend fun query / Flow streaming, idiomatic exception model. " +
                    "Java users should prefer dbfy-jvm directly."
                )
                url.set("https://github.com/typeeffect/dbfy")
                licenses {
                    license {
                        name.set("Apache-2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                developers {
                    developer { id.set("typeeffect"); name.set("typeeffect") }
                }
                scm {
                    connection.set("scm:git:https://github.com/typeeffect/dbfy.git")
                    developerConnection.set("scm:git:git@github.com:typeeffect/dbfy.git")
                    url.set("https://github.com/typeeffect/dbfy")
                }
            }
        }
    }
}

signing {
    val signingKey: String? = providers.environmentVariable("SIGNING_KEY").orNull
    val signingPassword: String? = providers.environmentVariable("SIGNING_PASSWORD").orNull
    if (signingKey != null && signingPassword != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["dbfyKotlin"])
    } else {
        logger.lifecycle("dbfy-kotlin: signing skipped (no SIGNING_KEY env var)")
    }
}

tasks.test {
    useJUnitPlatform()
    val nativeDir = providers.environmentVariable("DBFY_NATIVE_DIR")
        .getOrElse(layout.projectDirectory.dir("../../../target/release").asFile.absolutePath)
    systemProperty("java.library.path", nativeDir)
    jvmArgs("-Djava.library.path=$nativeDir")
}
