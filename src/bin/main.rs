extern crate elastictea;
extern crate rettle;
extern crate serde;

use elastictea::fill::{FillEsArg, FillEsTea, EsClient};
use rettle::tea::Tea;
use rettle::brewer::Brewery;
use rettle::pot::Pot;
use rettle::ingredient::{Argument, Steep};

use std::any::Any;
use std::time::Instant;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, ElasticType, Debug)]
struct ElasticTea {
    x: Option<i32>,
    str: Option<String>,
    test: Option<String>,
    y: Option<f32>
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
                                         {
                                             "match_all": {}
                                         },
                                         es_client
                                        );


    let brewery = Brewery::new(4, Instant::now());
    let mut new_pot = Pot::new();
    let fill_elastictea = FillEsTea::new<ElasticTea>("elastic_tea_test", "test_index", test_fill_esarg);

    new_pot.add_source(fill_elastictea);
    new_pot.add_ingredient(Box::new(Pour{
        name: String::from("pour1"),
        computation: Box::new(|tea_batch, args| {
            tea_batch.into_iter()
                .map(|tea| {
                    println!("{:?}", tea);
                })
        }),
        params: None,
    }));

    // Iterate through the hits in the response.
    //println!("{:?}", res);
    //for hit in res.hits() {
    //    println!("{:?}", hit);
    //}
    
}
