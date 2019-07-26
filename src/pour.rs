use crate::client::EsClient;

use rettle::ingredient::{Argument, Pour};
use rettle::tea::Tea;

use serde::Serialize;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use elastic::prelude::*;
use elastic::error::Error;

///
/// Ingredient params for PourEsTea.
pub struct PourEsArg {
    doc_index: &'static str,
    doc_type: &'static str,
    es_client: Arc<EsClient>,
}

impl PourEsArg {
    ///
    /// Returns a PourEsArg to be used as params in PourEsTea.
    ///
    /// # Arguments
    ///
    /// * `doc_index` - Elasticsearch index to send data to.
    /// * `doc_type` - Elasticsearch doc type to send data to.
    /// * `es_client` - EsClient used to request docs from.
    pub fn new(doc_index: &'static str, doc_type: &'static str, es_client: Arc<EsClient>) -> PourEsArg {
        PourEsArg { doc_index, doc_type, es_client }
    }
}

impl Argument for PourEsArg {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct PourEsTea {}

///
/// Wrapper to simplifiy the creation of the Pour Ingredient to be used in the rettle Pot.
impl PourEsTea {
    ///
    /// Returns the Pour Ingredient to be added to the `rettle` Pot.
    ///
    /// # Arguments
    ///
    /// * `name` - Ingredient name.
    /// * `source` - Ingredient source.
    /// * `params` - Params data structure holding the EsClient and params for sending docs.
    pub fn new<T: Tea + Send + Serialize + 'static>(name: &str, params: PourEsArg) -> Box<Pour> {
        Box::new(Pour {
            name: String::from(name),
            computation: Box::new(|tea_batch, args| {
                pour_to_es::<T>(tea_batch, args)
            }),
            params: Some(Box::new(params))
        })
    }
}

///
/// Implements the ES bulk insert request.
///
/// # Arguments
///
/// * `tea_batch` - Current batch of tea to be sent as a bulk request to ES.
/// * `args` - Params specifying the EsClient and query params to get docs.
fn pour_to_es<T: Tea + Send + Debug + Serialize + 'static>(tea_batch: Vec<Box<dyn Tea + Send>>, args: &Option<Box<dyn Argument + Send>>) -> Vec<Box<dyn Tea + Send>> {
    match args {
        None => panic!("Need to pass \"PourEsArg\" configs to run this Pour operation"),
        Some(box_args) => {
            // unwrap params and unpack them
            let box_args = box_args.as_any().downcast_ref::<PourEsArg>().unwrap();
            let PourEsArg { doc_index, doc_type, es_client } = box_args;
            let es_client = &es_client.client;

            // Format tea_batch as bulk request
            let bulk_req = tea_batch.iter()
                .map(|tea| {
                    let tea = tea.as_any().downcast_ref::<T>().unwrap();
                    bulk_raw().index(tea)
                });

            // Send bulk request to ES
            let res = es_client
                .bulk()
                .index(*doc_index)
                .ty(*doc_type)
                .extend(bulk_req)
                .send();

            // Inspect res to find errors.
            match res {
                Ok(res) => {
                    if res.is_err(){
                        println!("Some bulk insert items failed!");
                        for doc in res {
                            match doc {
                                Ok(_) => (),
                                Err(doc) => println!("Failed to insert doc: {:?}", doc),
                            }
                        }
                    }
                },
                Err(Error::Api(e)) => println!("Failed to send bulk request! REST API Error: {}", e),
                Err(e) => println!("HTTP or JSON failure! Error: {}", e),
            }
                    

            tea_batch
                
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PourEsArg, PourEsTea};
    use crate::client::EsClient;
    use rettle::tea::Tea;
    use rettle::pot::Pot;
    use serde::Serialize;
    use serde_json::json;
    use std::any::Any;
    use std::sync::Arc;

    #[derive(Default, Clone, Debug, Serialize)]
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
        let es_args = PourEsArg::new(
            "test_index", 
            "_doc",
            Arc::clone(&es_client),
        );
        assert_eq!(es_args.doc_index, "test_index");
        assert_eq!(es_args.doc_type, "_doc");
    }

    #[test]
    fn create_fill_estea() {
        let es_client = Arc::new(EsClient::new("test:test"));
        let es_args = PourEsArg::new(
            "test_index", 
            "_doc",
            Arc::clone(&es_client),
        );
        let pour_estea = PourEsTea::new::<TestEsTea>("test_es", es_args);
        let mut new_pot = Pot::new();
        new_pot.add_ingredient(pour_estea);
        assert_eq!(new_pot.get_recipe().read().unwrap().len(), 1);
        assert_eq!(new_pot.get_recipe().read().unwrap()[0].get_name(), "test_es");
    }

}

