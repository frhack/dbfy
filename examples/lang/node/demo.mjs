// Run with:
//   npm install @typeeffect/dbfy
//   node examples/lang/node/demo.mjs

import { Engine } from '@typeeffect/dbfy';
import { tableFromIPC } from 'apache-arrow';

const yaml = `
version: 1
sources:
  posts:
    type: rest
    base_url: https://jsonplaceholder.typicode.com
    tables:
      posts:
        endpoint: { method: GET, path: /posts }
        root: "$[*]"
        columns:
          id:     { path: "$.id",     type: int64  }
          userId: { path: "$.userId", type: int64  }
          title:  { path: "$.title",  type: string }
        pushdown:
          filters:
            userId: { param: userId, operators: ["="] }
`;

const engine = Engine.fromYaml(yaml);

// async — Promise resolved on V8 main thread via napi-rs
// ThreadsafeFunction. The JS event loop is never parked.
const buf = await engine.query(
    'SELECT id, title FROM posts.posts WHERE userId = 1 LIMIT 3',
);

const table = tableFromIPC(buf);
for (const row of table) {
    console.log(`${String(row.id).padStart(3)} | ${row.title}`);
}
