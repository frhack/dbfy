// `dbfy-jvm` — pure-Java artifact + per-platform native classifier
// jars. Java consumers just need this single coordinate (plus the
// classifier matching their OS); Kotlin consumers should pull
// `dbfy-kotlin` instead (which transitively pulls this).

plugins {
    `java-library`
    `maven-publish`
    signing
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
    withSourcesJar()
    withJavadocJar()
}

// --- Native classifier jars ----------------------------------------
//
// We don't bundle every platform's native lib in the main jar — that
// would balloon the artifact past 250 MB. Instead, each platform gets
// its own classifier jar containing only the native at
// `com/dbfy/native/<rid>/<libname>`. Consumers add the classifier
// matching their OS:
//
//   <dependency>
//     <groupId>io.github.typeeffect</groupId>
//     <artifactId>dbfy-jvm</artifactId>
//     <version>0.3.0</version>
//   </dependency>
//   <dependency>
//     <groupId>io.github.typeeffect</groupId>
//     <artifactId>dbfy-jvm</artifactId>
//     <version>0.3.0</version>
//     <classifier>natives-linux-x86_64</classifier>
//   </dependency>
//
// The CI workflow populates `src/main/native/<rid>/` on each runner
// from its own `cargo build -p dbfy-jni --release` output, then
// invokes the right `nativesJar*` task per matrix shard.

val nativeRids = listOf(
    "linux-x86_64", "linux-arm64",
    "osx-x86_64", "osx-arm64",
    "windows-x86_64",
)

fun rid2Lib(rid: String) = when {
    rid.startsWith("linux") -> "libdbfy_jni.so"
    rid.startsWith("osx") -> "libdbfy_jni.dylib"
    rid.startsWith("windows") -> "dbfy_jni.dll"
    else -> error("unknown rid: $rid")
}

val nativesJars = nativeRids.associateWith { rid ->
    tasks.register<Jar>("nativesJar-$rid") {
        archiveClassifier.set("natives-$rid")
        from(layout.projectDirectory.dir("src/main/native/$rid")) {
            into("com/dbfy/native/$rid")
            include(rid2Lib(rid))
        }
    }
}

publishing {
    publications {
        create<MavenPublication>("dbfyJvm") {
            artifactId = "dbfy-jvm"
            from(components["java"])
            // Attach all native-classifier jars; only those whose
            // source dir is populated on this runner will produce a
            // non-empty archive. The publish task uploads them all.
            nativesJars.forEach { (_, taskProvider) ->
                artifact(taskProvider.get())
            }
            pom {
                name.set("dbfy-jvm")
                description.set(
                    "Each source is a SQL table: REST APIs, log files, " +
                    "Postgres, LDAP, Parquet — one schema, cross-source JOINs. " +
                    "Java JNI bindings over the embedded dbfy engine."
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
    val signingKey: String? = providers.environmentVariable("SIGNING_KEY")
        .orNull
    val signingPassword: String? = providers.environmentVariable("SIGNING_PASSWORD")
        .orNull
    if (signingKey != null && signingPassword != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["dbfyJvm"])
    } else {
        // Locally we just compile + run unit tests, no signing.
        logger.lifecycle("dbfy-jvm: signing skipped (no SIGNING_KEY env var)")
    }
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    // The unit test wants the freshly-built `libdbfy_jni.so` reachable
    // via `java.library.path` so the loader's fallback path works
    // when no natives jar is on the classpath. CI exports
    // DBFY_NATIVE_DIR pointing at `target/release`; the gradle build
    // copies that into `src/main/native/<rid>/` for the classifier
    // jar at the same time.
    val nativeDir = providers.environmentVariable("DBFY_NATIVE_DIR")
        .getOrElse(layout.projectDirectory.dir("../../../target/release").asFile.absolutePath)
    systemProperty("java.library.path", nativeDir)
    jvmArgs("-Djava.library.path=$nativeDir")
}
