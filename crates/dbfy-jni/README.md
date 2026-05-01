# dbfy (Java)

JNI bindings for the embedded dbfy federated SQL engine.

## Build

The Rust side does not require a JDK to compile; the `jni` crate ships its own
header definitions:

```bash
cargo build -p dbfy-jni            # produces libdbfy_jni.{so,dylib,dll}
```

The Java side requires a JDK (any version >= 11):

```bash
javac -d crates/dbfy-jni/java/out crates/dbfy-jni/java/com/dbfy/Dbfy.java
```

## Run

The JVM must locate the native library. Either copy `libdbfy_jni.so` (Linux)
into a directory on `java.library.path`, or pass it explicitly:

```bash
java -cp crates/dbfy-jni/java/out \
     -Djava.library.path=target/debug \
     com.dbfy.Dbfy
```

(There is no `main` in `Dbfy.java`; embed it from your own Java program.)

## Usage

```java
import com.dbfy.Dbfy;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import java.io.ByteArrayInputStream;

try (Dbfy engine = Dbfy.fromYaml(yaml);
     RootAllocator allocator = new RootAllocator()) {

    byte[] ipc = engine.queryArrowIpc("SELECT id, name FROM crm.customers LIMIT 10");
    try (ArrowStreamReader reader = new ArrowStreamReader(
             new ByteArrayInputStream(ipc), allocator)) {
        while (reader.loadNextBatch()) {
            // reader.getVectorSchemaRoot() exposes the columns
        }
    }

    String plan = engine.explain("SELECT id FROM crm.customers");
}
```

## Why Arrow IPC bytes?

The Arrow C Data Interface (used by the Python and C bindings for zero-copy)
requires the JVM to also speak that interface. It works through
[`arrow-c-data`](https://arrow.apache.org/docs/java/cdata.html) but it adds
runtime complexity. Arrow IPC is the standard JVM exchange format and
`arrow-vector` already ships a streaming reader. Trade: one extra
serialise/deserialise pass; gain: simpler integration.

## Status

This is a scaffolding milestone: the Rust crate compiles and the API surface
mirrors the Python bindings (sync only). Async, programmatic providers, and a
Java-side integration test (which would require an end-to-end JVM run on this
machine) are deferred follow-ups.
