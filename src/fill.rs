use crate::client::EsClient;

use rettle::ingredient::{Ingredient, Argument, Fill};
use rettle::brewer::{Brewery, make_tea};
use rettle::tea::Tea;

use std::sync::{Arc, RwLock};
use std::any::Any;
use serde::Deserialize;
use std::fmt::Debug;
use serde_json::{Value, json};

///
/// Ingredient params for FillEsTea.
pub struct FillEsArg {
    doc_index: &'static str,
    doc_type: &'static str,
    num_docs: usize,
    query: Value,
    es_client: Arc<EsClient>,
}

impl FillEsArg {
    ///
    /// Returns a FillEsArg to be used as params in FillEsTea.
    ///
    /// # Arguments
    ///
    /// * `doc_index` - Elasticsearch index to pull data from.
    /// * `doc_type` - Elasticsearch doc type to pull data from.
    /// * `num_docs` - Number of docs to pull in each batch.
    /// * `query` - Query to run and match Elasticsearch docs against.
    /// * `es_client` - EsClient used to request docs from.
    pub fn new(doc_index: &'static str, doc_type: &'static str, num_docs: usize, query: Value, es_client: Arc<EsClient>) -> FillEsArg {
        FillEsArg { doc_index, doc_type, num_docs, query, es_client }
    }
}

impl Argument for FillEsArg {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct FillEsTea {}

///
/// Wrapper to simplifiy the creation of the Fill Ingredient to be used in the rettle Pot.
impl FillEsTea {
    ///
    /// Returns the Fill Ingredient to be added to the `rettle` Pot.
    ///
    /// # Arguments
    ///
    /// * `name` - Ingredient name.
    /// * `source` - Ingredient source.
    /// * `params` - Params data structure holding the EsClient and query params for pulling docs.
    pub fn new<T: Tea + Send + 'static>(name: &str, source: &str, params: FillEsArg) -> Box<Fill> 
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

///
/// Implements the ES request, deserialization to specified data struct, and passes the data to the
/// brewery for processing.
///
/// # Arguments
///
/// * `args` - Params specifying the EsClient and query params to get docs.
/// * `brewery` - Brewery that processes the data.
/// * `recipe` - Recipe for the ETL used by the Brewery.
fn fill_from_es<T: Tea + Send + Debug + 'static>(args: &Option<Box<dyn Argument + Send>>, brewery: &Brewery, recipe: Arc<RwLock<Vec<Box<dyn Ingredient + Send + Sync>>>>) 
    where for<'de> T: Deserialize<'de>
{
    match args {
        None => panic!("Need to pass \"FillEsArg\" configs to run this Fill operation"),
        Some(box_args) => {
            // unwrap params and unpack them
            let box_args = box_args.as_any().downcast_ref::<FillEsArg>().unwrap();
            let FillEsArg { doc_index, doc_type, num_docs, query, es_client } = box_args;
            let es_client = &es_client.client;

            // loop over the data in batches, sending to the brewery
            let mut start_pos = 0;
            loop {
                // get docs from Elasticsearch client
                let res = es_client
                    .search::<T>()
                    .index(*doc_index)
                    .ty(*doc_type)
                    .body(
                        json!({
                            "from": start_pos,
                            "size": *num_docs,
                            "query": *query
                        })
                    )
                    .send()
                    .unwrap();
                
                let tea_batch: Vec<Box<dyn Tea + Send>> = res
                    .into_documents()
                    .map(|tea| {
                        Box::new(tea) as Box<dyn Tea + Send>
                    })
                    .collect();

                // If docs are found, send to brewery for processing.
                if tea_batch.len() == 0 {
                    break;
                } else {
                    let recipe = Arc::clone(&recipe);
                    call_brewery(brewery, recipe, tea_batch);
                    start_pos += *num_docs;
                }

                // Break if doc offset + size is > 10000.
                // TODO: When scroll is supported, this can be removed
                if start_pos + num_docs > 10000 {
                    break;
                }
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FillEsArg, FillEsTea};
    use crate::client::EsClient;
    use rettle::tea::Tea;
    use rettle::pot::Pot;
    use serde::Deserialize;
    use serde_json::json;
    use std::any::Any;
    use std::sync::Arc;

    #[derive(Default, Clone, Debug, Deserialize)]
    struct TestEsTea {
        name: String,
        value: i32
    }

    impl Tea for TestEsTea {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn new(self: Box<Self>) -> Box<dyn Tea + Send> {
            self
        }
    }

    #[test]
    fn create_es_args() {
        let es_client = Arc::new(EsClient::new("test:test"));
        let es_args = FillEsArg::new(
            "test_index", 
            "_doc",
            50,
            json!({
                "match_all": {}
            }),
            Arc::clone(&es_client),
        );
        assert_eq!(es_args.doc_index, "test_index");
        assert_eq!(es_args.doc_type, "_doc");
    }

    #[test]
    fn create_fill_estea() {
        let es_client = Arc::new(EsClient::new("test:test"));
        let es_args = FillEsArg::new(
            "test_index", 
            "_doc",
            50,
            json!({
                "match_all": {}
            }),
            Arc::clone(&es_client),
        );
        let fill_estea = FillEsTea::new::<TestEsTea>("test_es", "fixture", es_args);
        let mut new_pot = Pot::new();
        new_pot.add_source(fill_estea);
        assert_eq!(new_pot.get_sources().len(), 1);
        assert_eq!(new_pot.get_sources()[0].get_name(), "test_es");
    }

}
