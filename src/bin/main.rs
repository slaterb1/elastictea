extern crate elastictea;
extern crate rettle;
extern crate serde;

#[macro_use]
extern crate elastic_derive;
#[macro_use]
extern crate serde_json;

use elastictea::fill::{FillEsArg, FillEsTea, EsClient};
use rettle::tea::Tea;
use rettle::brewer::Brewery;
use rettle::pot::Pot;
use rettle::ingredient::Pour;

use std::any::Any;
use std::time::Instant;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, ElasticType, Debug)]
struct ElasticTea {
    ListingId: Option<String>,
    ListPrice: Option<f32>,
    City: Option<String>,
    BathroomsTotalInteger: Option<String>
}

impl Tea for ElasticTea {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn new(self: Box<Self>) -> Box<dyn Tea + Send> {
        self
    }
}

fn main() {
    let es_client = EsClient::new("http://localhost:9200");
    let test_fill_esarg = FillEsArg::new("test-index1",
                                         "_doc",
                                         10,
                                         json!({
                                             "query_string": {
                                                 "query": "*"
                                             }
                                         }),
                                         es_client
                                        );


    let brewery = Brewery::new(4, Instant::now());
    let mut new_pot = Pot::new();
    let fill_elastictea = FillEsTea::new::<ElasticTea>("elastic_tea_test", "test_index", test_fill_esarg);

    new_pot.add_source(fill_elastictea);
    new_pot.add_ingredient(Box::new(Pour{
        name: String::from("pour1"),
        computation: Box::new(|tea_batch, args| {
            tea_batch.into_iter()
                .map(|tea| {
                    println!("{:?}", tea);
                    tea
                })
                .collect()
        }),
        params: None,
    }));
    new_pot.brew(&brewery);

    // Iterate through the hits in the response.
    //println!("{:?}", res);
    //for hit in res.hits() {
    //    println!("{:?}", hit);
    //}
    
}
