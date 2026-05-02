# Publishing dbfy-jvm + dbfy-kotlin to Maven Central

The CI workflow `.github/workflows/maven-jvm.yml` automates the build
and upload, but it depends on **GitHub repo secrets** that have to be
set up by hand once. This document walks through that one-time setup.

## 1. Sonatype Central namespace

Maven Central is governed by [central.sonatype.com](https://central.sonatype.com)
(the legacy OSSRH portal was deprecated mid-2025).

We chose `io.github.typeeffect` as the groupId precisely because
**it auto-verifies via GitHub OAuth** — Sonatype maps the namespace
to the `typeeffect` GitHub org and checks ownership through the
sign-in flow. No JIRA ticket, no DNS TXT record, no temporary repo
dance. Total setup time: ~2 minutes.

1. Sign in at <https://central.sonatype.com> using "Sign in with
   GitHub" and authorise the `typeeffect` org.
2. Click **Add Namespace** and enter `io.github.typeeffect`. The
   portal verifies it instantly because you signed in via the same
   GitHub identity that owns the org.
3. Generate a **user token** under your profile → "Generate User
   Token". This produces a `username` + `password` pair —
   these are *not* your portal login; they are credentials the CI
   uses to upload artifacts.

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
git tag -a v0.4.1 -m "v0.4.1"
git push --tags
```

The `maven-jvm` workflow runs the matrix build, gathers the natives,
signs everything, and uploads. The first deployment to Sonatype
Central goes into the **portal review queue** — log into
central.sonatype.com to "promote" it to publication. Subsequent
uploads under the same namespace publish immediately.

After ~10 minutes, the artifacts appear at
<https://repo1.maven.org/maven2/io/github/typeeffect/dbfy-jvm/>.

## 5. Consumer install snippets

**Maven**:
```xml
<dependency>
  <groupId>io.github.typeeffect</groupId>
  <artifactId>dbfy-jvm</artifactId>
  <version>0.4.1</version>
</dependency>
<dependency>
  <groupId>io.github.typeeffect</groupId>
  <artifactId>dbfy-jvm</artifactId>
  <version>0.4.1</version>
  <classifier>natives-linux-x86_64</classifier>
  <scope>runtime</scope>
</dependency>
```

**Gradle (Kotlin DSL)**:
```kotlin
dependencies {
    implementation("io.github.typeeffect:dbfy-jvm:0.4.1")
    runtimeOnly("io.github.typeeffect:dbfy-jvm:0.4.1:natives-linux-x86_64")
    // For Kotlin code, prefer:
    // implementation("io.github.typeeffect:dbfy-kotlin:0.4.1")
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
