/*!
# elastictea
Generic Fill Pour Ingredient crate for the `rettle` ETL.

## Data Structures
- FillEsArg: Ingredient params for FillEsTea
- FillEsTea: Wrapper to simplifiy the creation of the Fill Ingredient to be used in the rettle Pot.
- PourEsArg: Ingredient params for PourEsTea
- PourEsTea: Wrapper to simplifiy the creation of the Pour Ingredient to be used in the rettle Pot.

## Example
```ignore
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

    new_pot.add_source(fill_elastictea);

    // Steep operations of choice
    
    new_pot.add_ingredient(pour_elastictea);

    new_pot.brew(&brewery);
}
```
*/

pub mod client;
pub mod fill;
pub mod pour;

// Re-exports
pub use self::fill::{FillEsArg, FillEsTea};
pub use self::pour::{PourEsArg, PourEsTea};
