#[macro_use]
extern crate elastic_derive;
#[macro_use]
extern crate serde_json;
extern crate elastic;
extern crate serde;

use elastic::prelude::*;
//use serde_json::Value;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, ElasticType, Debug)]
struct ElasticTea {
    x: Option<i32>,
    str: Option<String>,
    test: Option<String>,
    y: Option<f32>
}

fn main() {
    let client = SyncClientBuilder::new()
        .static_node("http://localhost:9200")
        .params_fluent(|p| p.url_param("pretty", true))
        .build()
        .unwrap();

    // A search request with a freeform body.
    let res = client.search::<ElasticTea>()
                    .index("test-index")
                    .body(json!({
                        "query": {
                            "bool": {
                                "must": {
                                    "match": {
                                        "x": 5
                                    }
                                }
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
