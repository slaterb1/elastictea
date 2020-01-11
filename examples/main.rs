use elastictea::fill::{FillEsArg, FillEsTea};
use elastictea::pour::{PourEsArg, PourEsTea};
use elastictea::client::EsClient;

use rettle::{
    Brewery,
    Pot,
};

use std::sync::Arc;
use serde::{Serialize, Deserialize};
use serde_json::json;

#[derive(Serialize, Deserialize, Debug)]
struct ElasticTea {
    name: Option<String>,
    avg: Option<f32>,
}

fn main() {
    let es_client = Arc::new(EsClient::new("http://localhost:9200"));
    let test_fill_esarg = FillEsArg::new(
        "test-fill-index",
        "_doc",
        200,
        json!({
            "match_all": {}
        }),
        Arc::clone(&es_client),
    );

    let test_pour_esarg = PourEsArg::new(
        "test-pour-index",
        "_doc",
        Arc::clone(&es_client),
    );



    let brewery = Brewery::new(4);
    let fill_elastictea = FillEsTea::new::<ElasticTea>("elastic_tea_test", "test_index", test_fill_esarg);
    let pour_elastictea = PourEsTea::new::<ElasticTea>("pour_elastic", test_pour_esarg);

    let new_pot = Pot::new()
        .add_source(fill_elastictea)
        .add_ingredient(pour_elastictea);
    new_pot.brew(&brewery);
}
