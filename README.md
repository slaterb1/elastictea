# elastictea

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.com/slaterb1/elastictea.svg?branch=master)](https://travis-ci.com/slaterb1/elastictea)
[![Crates.io Version](https://img.shields.io/crates/v/elastictea.svg)](https://crates.io/crates/elastictea)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.35.0+-lightgray.svg)](#rust-version-requirements)

Generic Fill Pour Ingredient crate for the `rettle` ETL.

## Data Structures
- FillEsArg: Ingredient params for FillEsTea
- FillEsTea: Wrapper to simplifiy the creation of the Fill Ingredient to be used in the rettle Pot.
- PourEsArg: Ingredient params for PourEsTea
- PourEsTea: Wrapper to simplifiy the creation of the Pour Ingredient to be used in the rettle Pot.

## Example
```rust
#[derive(Serialize, Deserialize, Debug)]
struct ElasticTea {
    name: Option<String>,
    avg: Option<f32>,
}

impl Tea for ElasticTea {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn main() {
    let es_client = Arc::new(EsClient::new("http://localhost:9200"));
    let test_fill_esarg = FillEsArg::new(
        "test-index1",
        "_doc",
        200,
        json!({
            "match_all": {}
        }),
        Arc::clone(&es_client),
    );

    let test_pour_esarg = PourEsArg::new(
        "test-pour-index2",
        "_doc",
        Arc::clone(&es_client),
    );



    let brewery = Brewery::new(4, Instant::now());
    let mut new_pot = Pot::new();
    let fill_elastictea = FillEsTea::new::<ElasticTea>("elastic_tea_test", "test_index", test_fill_esarg);
    let pour_elastictea = PourEsTea::new::<ElasticTea>("pour_elastic", test_pour_esarg);

    new_pot = new_pot.add_source(fill_elastictea);

    // Steep operations of choice
    
    new_pot = new_pot.add_ingredient(pour_elastictea);

    new_pot.brew(&brewery);
}
```
