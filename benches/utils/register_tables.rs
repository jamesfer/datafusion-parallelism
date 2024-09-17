use std::sync::Arc;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::execution::context::SessionState;
use crate::utils::static_table::StaticTable;

pub fn register_tables(session_state: &SessionState, tables: Vec<(String, SchemaRef, Vec<RecordBatch>)>) {
    // Register all tables
    let schema_provider = Arc::new(MemorySchemaProvider::new());
    for (name, schema, data) in tables {
        let table = StaticTable::new(schema, data);
        schema_provider.register_table(name.to_string(), Arc::new(table)).unwrap();
    }

    // Register schema
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog.register_schema("my_schema", schema_provider)
        .map_err(|err| format!("register schema error: {}", err))
        .unwrap();

    // Register catalog
    session_state.catalog_list().register_catalog("my_catalog".to_string(), catalog);
}
