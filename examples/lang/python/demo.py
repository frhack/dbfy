"""Run with: python3 examples/lang/python/demo.py

Requires `pip install dbfy` from PyPI (or `maturin develop` from
crates/dbfy-py for local dev).
"""

import dbfy

YAML = """
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
"""


def main() -> None:
    engine = dbfy.Engine.from_yaml(YAML)
    table = engine.query("SELECT id, title FROM posts.posts WHERE userId = 1 LIMIT 3")
    # `table` is a pyarrow.Table; iterate rows naturally.
    for batch in table.to_batches():
        ids = batch.column("id")
        titles = batch.column("title")
        for i in range(batch.num_rows):
            print(f"{ids[i].as_py():>3} | {titles[i].as_py()}")


if __name__ == "__main__":
    main()
