use std::fmt::Debug;

use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::SendableRecordBatchStream;
use crate::operator::lookup_consumers::{IndexLookupConsumer, IndexLookupProvider};
use crate::parse_sql::JoinReplacement;

use crate::operator::version1::build_implementation::Version1;
use crate::operator::version2::build_implementation::Version2;
use crate::operator::version3::build_implementation::Version3;
use crate::operator::version4::build_implementation::Version4;
use crate::operator::version5::inner_hash_join::Version5;
use crate::operator::version6::inner_hash_join::Version6;
use crate::operator::version7::inner_hash_join::Version7;
use crate::operator::version8::build_implementation::Version8;
use crate::operator::version9::build_implementation::Version9;

#[derive(Debug)]
pub enum BuildImplementation {
    Version1(Version1),
    Version2(Version2),
    Version3(Version3),
    Version4(Version4),
    Version5(Version5),
    Version6(Version6),
    Version7(Version7),
    Version8(Version8),
    Version9(Version9),
}

impl BuildImplementation {
    pub fn new(build_implementation_version: JoinReplacement, parallelism: usize) -> Self {
        match build_implementation_version {
            JoinReplacement::Original => BuildImplementation::Version1(Version1::new(parallelism)),
            JoinReplacement::New => BuildImplementation::Version2(Version2::new(parallelism)),
            JoinReplacement::New3 => BuildImplementation::Version3(Version3::new(parallelism)),
            JoinReplacement::New4 => BuildImplementation::Version4(Version4::new(parallelism)),
            JoinReplacement::New5 => BuildImplementation::Version5(Version5::new(parallelism)),
            JoinReplacement::New6 => BuildImplementation::Version6(Version6::new(parallelism)),
            JoinReplacement::New7 => BuildImplementation::Version7(Version7::new(parallelism)),
            JoinReplacement::New8 => BuildImplementation::Version8(Version8::new(parallelism)),
            JoinReplacement::New9 => BuildImplementation::Version9(Version9::new(parallelism)),
        }
    }

    pub async fn build_side<Consumer>(
        &self,
        partition: usize,
        stream: SendableRecordBatchStream,
        build_expressions: &Vec<PhysicalExprRef>,
        consumer: Consumer
    ) -> Result<Consumer::R, DataFusionError>
        where Consumer: IndexLookupConsumer + Send,
    {
        // TODO change all method names
        match self {
            BuildImplementation::Version1(version1) => version1.build_lookup_map(
                partition,
                stream,
                build_expressions,
            ).await.map(|provider| provider.consume(consumer)),
            BuildImplementation::Version2(implementation) => implementation.build_lookup_map(
                partition,
                stream,
                build_expressions,
            ).await.map(|provider| provider.consume(consumer)),
            BuildImplementation::Version3(implementation) => implementation.build_lookup_map(
                partition,
                stream,
                build_expressions,
            ).await.map(|provider| provider.consume(consumer)),
            BuildImplementation::Version4(implementation) => implementation.build_right_side(
                partition,
                stream,
                build_expressions,
            ).await.map(|provider| provider.consume(consumer)),
            BuildImplementation::Version5(implementation) => implementation.build_right_side(
                partition,
                stream,
                build_expressions,
            ).await.map(|provider| provider.consume(consumer)),
            BuildImplementation::Version6(implementation) => implementation.build_right_side(
                partition,
                stream,
                build_expressions,
            ).await.map(|provider| provider.consume(consumer)),
            BuildImplementation::Version7(implementation) => implementation.build_right_side(
                partition,
                stream,
                build_expressions,
            ).await.map(|provider| provider.consume(consumer)),
            BuildImplementation::Version8(implementation) => implementation.build_lookup_map(
                partition,
                stream,
                build_expressions,
            ).await.map(|provider| provider.consume(consumer)),
            BuildImplementation::Version9(implementation) => implementation.build_lookup_map(
                partition,
                stream,
                build_expressions,
            ).await.map(|provider| provider.consume(consumer)),
        }
    }
}
