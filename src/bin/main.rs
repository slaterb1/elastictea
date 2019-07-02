#[macro_use]
extern crate elastic_derive;
#[macro_use]
extern crate serde_json;
extern crate elastic;

use elastic::prelude::*;
use serde_json::Value;

fn main() {
    println!("Test elastictea crate");
    let client = SyncClientBuilder::new().build().unwrap();
    let query = "some query string";

    // A search request with a freeform body.
    let res = client.search::<Value>()
                    .index("test-index")
                    .body(json!({
                        "query": {
                            "query_string": {
                                "query": query
                            }
                        }
                    }))
                    .send()
                    .unwrap();

    // Iterate through the hits in the response.
    for hit in res.hits() {
        println!("{:?}", hit);
    }
    
}
