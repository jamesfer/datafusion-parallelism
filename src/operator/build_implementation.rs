use std::fmt::Debug;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::SendableRecordBatchStream;
use crate::parse_sql::JoinReplacement;

use crate::utils::index_lookup::IndexLookup;
use crate::version1::inner_hash_join::Version1;
use crate::version2::inner_hash_join::Version2;
use crate::version3::inner_hash_join::Version3;

pub trait IndexLookupConsumer {
    type R;

    fn call<Lookup>(self, index_lookup: Lookup, record_batch: RecordBatch) -> Self::R
        where Lookup: IndexLookup<u64> + Sync + Send + 'static;
}

#[derive(Debug)]
pub enum BuildImplementation {
    Version1(Version1),
    Version2(Version2),
    Version3(Version3),
}

impl BuildImplementation {
    pub fn new(build_implementation_version: JoinReplacement, parallelism: usize) -> Self {
        match build_implementation_version {
            JoinReplacement::Original => BuildImplementation::Version1(Version1::new(parallelism)),
            JoinReplacement::New => BuildImplementation::Version2(Version2::new(parallelism)),
            JoinReplacement::New3 => BuildImplementation::Version3(Version3::new(parallelism)),
        }
    }

    pub async fn build_right_side<C>(
        &self,
        partition: usize,
        stream: SendableRecordBatchStream,
        build_expressions: &Vec<PhysicalExprRef>,
        consume: C,
    ) -> Result<C::R, DataFusionError>
        where C: IndexLookupConsumer + Send {
        match self {
            BuildImplementation::Version1(version1) => version1.build_right_side(
                partition,
                stream,
                build_expressions,
                consume,
            ).await,
            BuildImplementation::Version2(implementation) => implementation.build_right_side(
                partition,
                stream,
                build_expressions,
                consume,
            ).await,
            BuildImplementation::Version3(implementation) => implementation.build_right_side(
                partition,
                stream,
                build_expressions,
                consume,
            ).await,
        }
    }
}
