# Publishing dbfy-jvm + dbfy-kotlin to Maven Central

The CI workflow `.github/workflows/maven-jvm.yml` automates the build
and upload, but it depends on **GitHub repo secrets** that have to be
set up by hand once. This document walks through that one-time setup.

## 1. Sonatype Central namespace

Maven Central is now governed by [central.sonatype.com](https://central.sonatype.com)
(the legacy OSSRH portal is deprecated as of mid-2025).

1. Create an account at <https://central.sonatype.com>.
2. Register the namespace `com.dbfy`. Sonatype will ask you to
   prove ownership — for a non-domain namespace like `com.dbfy`,
   you must instead claim a `com.github.frhack` namespace by
   creating a public, *temporary* repo with a name they generate
   (e.g. `OSSRH-XXXXXX`). For a real domain, add a TXT record.
3. Wait for the namespace to be approved (usually <1h, sometimes
   24h).
4. Generate a **user token** under your profile → "Generate User
   Token". This produces a `username` + `password` pair —
   these are *not* your portal login; they are token credentials.

## 2. GPG key for artifact signing

Maven Central requires every uploaded artifact to be signed.

```bash
# Generate a 4096-bit RSA key, no expiration.
gpg --full-generate-key

# Find the key ID (long form):
gpg --list-secret-keys --keyid-format=long

# Publish the public key so the verifier can find it:
gpg --keyserver keys.openpgp.org --send-keys <KEY_ID>

# Export the secret key as ASCII-armored text:
gpg --export-secret-keys --armor <KEY_ID>
```

Save the ASCII-armored output (starts with
`-----BEGIN PGP PRIVATE KEY BLOCK-----`) — you'll paste it into the
GitHub secret below. The passphrase you set during key generation is
the second secret.

## 3. GitHub secrets

In the repo settings → Secrets and variables → Actions, add:

| Secret name              | Value                                                 |
|--------------------------|-------------------------------------------------------|
| `SONATYPE_USERNAME`      | The username from your Sonatype user token           |
| `SONATYPE_TOKEN`         | The password from your Sonatype user token           |
| `MAVEN_SIGNING_KEY`      | The ASCII-armored GPG private key from step 2         |
| `MAVEN_SIGNING_PASSWORD` | The GPG passphrase                                    |

## 4. First publish

```bash
git tag -a v0.3.0 -m "v0.3.0"
git push --tags
```

The `maven-jvm` workflow runs the matrix build, gathers the natives,
signs everything, and uploads. The first deployment to Sonatype
Central goes into the **portal review queue** — log into
central.sonatype.com to "promote" it to publication. Subsequent
uploads under the same namespace publish immediately.

After ~10 minutes, the artifacts appear at
<https://repo1.maven.org/maven2/com/dbfy/dbfy-jvm/>.

## 5. Consumer install snippets

**Maven**:
```xml
<dependency>
  <groupId>com.dbfy</groupId>
  <artifactId>dbfy-jvm</artifactId>
  <version>0.3.0</version>
</dependency>
<dependency>
  <groupId>com.dbfy</groupId>
  <artifactId>dbfy-jvm</artifactId>
  <version>0.3.0</version>
  <classifier>natives-linux-x86_64</classifier>
  <scope>runtime</scope>
</dependency>
```

**Gradle (Kotlin DSL)**:
```kotlin
dependencies {
    implementation("com.dbfy:dbfy-jvm:0.3.0")
    runtimeOnly("com.dbfy:dbfy-jvm:0.3.0:natives-linux-x86_64")
    // For Kotlin code, prefer:
    // implementation("com.dbfy:dbfy-kotlin:0.3.0")
}
```

The classifier names are `natives-linux-x86_64`, `natives-linux-arm64`,
`natives-osx-x86_64`, `natives-osx-arm64`, `natives-windows-x86_64`.

## Local development (no publishing)

```bash
cargo build -p dbfy-jni --release
cd bindings/jvm
DBFY_NATIVE_DIR=$PWD/../../target/release ./gradlew :dbfy-jvm:test
```

The `java.library.path` fallback in `Dbfy.loadNative()` picks up the
freshly-built `libdbfy_jni.so` so you can iterate without touching
the classifier-jar machinery.
