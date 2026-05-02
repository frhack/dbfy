// Run with:
//   cargo run --release -p dbfy-cli --bin dbfy_lang_rust
// or, if you'd rather have a standalone Cargo project, copy this
// file into a new bin crate that depends on
// `dbfy-frontend-datafusion` via path / git.

use dbfy_config::Config;
use dbfy_frontend_datafusion::Engine;

const YAML: &str = r#"
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
"#;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_yaml_str(YAML)?;
    let engine = Engine::from_config(config)?;

    let batches = engine
        .query("SELECT id, title FROM posts.posts WHERE userId = 1 LIMIT 3")
        .await?;

    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let titles = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            println!("{:>3} | {}", ids.value(i), titles.value(i));
        }
    }

    Ok(())
}
