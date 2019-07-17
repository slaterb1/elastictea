extern crate rettle;
extern crate elastic;

use rettle::ingredient::{Ingredient, Argument, Fill};
use rettle::brewer::{Brewery, make_tea};
use rettle::tea::Tea;

use std::sync::{Arc, RwLock};
use std::any::Any;
use serde::Deserialize;
use std::fmt::Debug;
use serde_json::Value;
use elastic::prelude::*;

///
/// Configs to setup Elasticsearch client.
pub struct EsClient {
    /// Elasticsearch host:port string.
    es_host: String,
    client: SyncClient,
}

impl EsClient {
    pub fn new(es_host: &str) -> EsClient {
        let client = SyncClientBuilder::new()
            .static_node(es_host)
            .params_fluent(|p| p.url_param("pretty", true))
            .build()
            .unwrap();

        EsClient { es_host: String::from(es_host), client }
    }
}

///
/// Ingredient params for FillEsTea.
pub struct FillEsArg {
    doc_index: &'static str,
    doc_type: &'static str,
    num_docs: u32,
    query: Value,
    es_client: EsClient,
}

impl FillEsArg {
    ///
    /// Returns a FillEsArg to be used as params in FillEsTea.
    ///
    /// # Arguments
    ///
    /// * `filepath` - filepath for csv to load.
    /// * `buffer_length` - number of csv lines to process at a time.
    pub fn new(doc_index: &'static str, doc_type: &'static str, num_docs: u32, query: Value, es_client: EsClient) -> FillEsArg {
        FillEsArg { doc_index, doc_type, num_docs, query, es_client }
    }
}

impl Argument for FillEsArg {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct FillEsTea {}

impl FillEsTea {
    pub fn new<T: Tea + Send + 'static>(name: &str, source: &str, params: FillEsArg, index: &'static str) -> Box<Fill> 
        where for<'de> T: Deserialize<'de>
    {
        Box::new(Fill {
            name: String::from(name),
            source: String::from(source),
            computation: Box::new(|args, brewery, recipe| {
                fill_from_es::<T>(args, brewery, recipe);
            }),
            params: Some(Box::new(params))
        })
    }
}

/// Helper function that sends to batch request to Brewers for processing.
///
/// # Arguments
///
/// * `brewery` - Brewery that processes the data.
/// * `recipe` - Recipe for the ETL used by the Brewery.
/// * `tea_batch` - Current batch to be sent and processed
fn call_brewery(brewery: &Brewery, recipe: Arc<RwLock<Vec<Box<dyn Ingredient + Send + Sync>>>>, tea_batch: Vec<Box<dyn Tea + Send>>) {
    brewery.take_order(|| {
        make_tea(tea_batch, recipe);
    });
}

fn fill_from_es<T: Tea + Send + Debug + ?Sized + 'static>(args: &Option<Box<dyn Argument + Send>>, brewery: &Brewery, recipe: Arc<RwLock<Vec<Box<dyn Ingredient + Send + Sync>>>>) 
    where for<'de> T: Deserialize<'de>
{
    match args {
        None => panic!("Need to pass \"FillEsArg\" configs to run this Fill operation"),
        Some(box_args) => {
            // unwrap params and unpack them
            let box_args = box_args.as_any().downcast_ref::<FillEsArg>().unwrap();

            // loop over the data in batches, sending to the brewery
            loop {
                let res = box_args.es_client.client.search::<T>()
                                                   .index(box_args.doc_index)
                                                   .ty(box_args.doc_type)
                                                   .body(json!({
                                                       "from": 0,
                                                       "size": box_args.num_docs,
                                                       "query": box_args.query
                                                   }))
                                                   .send()
                                                   .unwrap();
               println!("{:?}", res);
               break;
            }
        }
    }
}

