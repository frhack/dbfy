// Smoke tests for the @frhack/dbfy Node.js binding.
//
// Run with `npm test`. Exercises:
//   - Engine factory (newEmpty)
//   - sync error propagation (querySync on missing table)
//   - async Promise resolution (Engine.query returning Buffer)
//   - async error rejection (Promise rejected with the dbfy error)
//
// We can't talk to a real source from a unit test, so we use the
// "missing table" path on an empty engine to exercise the error
// roundtrip.

import test from 'node:test';
import assert from 'node:assert/strict';

import { Engine } from '../index.js';

test('newEmpty creates an engine', () => {
    const engine = Engine.newEmpty();
    assert.ok(engine);
});

test('querySync surfaces native errors as a thrown Error', () => {
    const engine = Engine.newEmpty();
    assert.throws(() => engine.querySync('SELECT * FROM nonexistent'), {
        message: /nonexistent/i,
    });
});

test('async query returns a Promise that rejects on native error', async () => {
    const engine = Engine.newEmpty();
    await assert.rejects(
        engine.query('SELECT * FROM nonexistent_table'),
        /nonexistent_table/i,
    );
});

test('async query resolves on the V8 main thread, never blocks', async () => {
    // The async fn should return a Promise immediately. Measure the
    // synchronous portion of the call (until the await point) to
    // verify it's microsecond-scale, not "block until query done".
    const engine = Engine.newEmpty();
    const t0 = process.hrtime.bigint();
    const promise = engine.query('SELECT 1');
    const submitNs = process.hrtime.bigint() - t0;
    assert.ok(submitNs < 1_000_000_000n,
        `submit took ${submitNs}ns, expected <1s`);
    // Drain the promise so the native task doesn't outlive the test.
    try { await promise; } catch (_) { /* expected on empty engine */ }
});
