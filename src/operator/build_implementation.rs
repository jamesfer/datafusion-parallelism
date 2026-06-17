use std::fmt::Debug;
use std::future::ready;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion_common::DataFusionError;
use datafusion::error::Result;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::SendableRecordBatchStream;
use futures_core::future::BoxFuture;
use crate::operator::lookup_consumers::{IndexLookupBorrower, IndexLookupConsumer, IndexLookupProvider};
use crate::operator::version10::build_implementation::Version10;
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
use crate::utils::index_lookup::IndexLookup;

#[async_trait]
pub trait CooperativeBuildVersion {
    type Map: IndexLookup<u64>;

    async fn build_lookup_map(
        &self,
        partition: usize,
        build_side_stream: SendableRecordBatchStream,
        build_expressions: &Vec<PhysicalExprRef>,
    ) -> Result<BoxFuture<'static, Result<(Self::Map, RecordBatch)>>>;
}

#[async_trait]
pub trait BuildVersion {
    type Map: IndexLookup<u64>;

    async fn build_lookup_map(
        &self,
        partition: usize,
        build_side_stream: SendableRecordBatchStream,
        build_expressions: &Vec<PhysicalExprRef>,
    ) -> Result<(Self::Map, RecordBatch)>;
}

#[async_trait]
impl <T: BuildVersion + Sync + Send + 'static> CooperativeBuildVersion for T
where T::Map: Send + 'static
{
    type Map = T::Map;

    async fn build_lookup_map(
        &self,
        partition: usize,
        build_side_stream: SendableRecordBatchStream,
        build_expressions: &Vec<PhysicalExprRef>
    ) -> Result<BoxFuture<'static, Result<(Self::Map, RecordBatch)>>> {
        let result = self.build_lookup_map(partition, build_side_stream, build_expressions).await?;
        Ok(Box::pin(ready(Ok(result))))
    }
}

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
    Version10(Version10),
}

impl BuildImplementation {
    pub fn new(build_implementation_version: JoinReplacement, parallelism: usize, input_schema: SchemaRef) -> Self {
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
            JoinReplacement::New10 => BuildImplementation::Version10(Version10::new(parallelism, input_schema)),
        }
    }

    // pub async fn build_side(
    //     &self,
    //     partition: usize,
    //     stream: SendableRecordBatchStream,
    //     build_expressions: &Vec<PhysicalExprRef>,
    // ) -> Result<(BuiltIndexLookup, RecordBatch)> {
    //     match self {
    //         BuildImplementation::Version1(version1) => {
    //             let (map, records) = version1.build_lookup_map(
    //                 partition,
    //                 stream,
    //                 build_expressions,
    //             ).await?;
    //             Ok((BuiltIndexLookup::Version1(map), records))
    //         },
    //         BuildImplementation::Version2(version2) => {
    //             let (map, records) = version2.build_lookup_map(
    //                 partition,
    //                 stream,
    //                 build_expressions,
    //             ).await?;
    //             Ok(consumer.call(map, records))
    //         },
    //         BuildImplementation::Version3(version3) => {
    //             let (map, records) = version3.build_lookup_map(
    //                 partition,
    //                 stream,
    //                 build_expressions,
    //             ).await?;
    //             Ok(consumer.call(map, records))
    //         },
    //         BuildImplementation::Version4(version4) => {
    //             let (map, records) = version4.build_lookup_map(
    //                 partition,
    //                 stream,
    //                 build_expressions,
    //             ).await?;
    //             Ok(consumer.call(map, records))
    //         },
    //         BuildImplementation::Version5(version5) => {
    //             let (map, records) = version5.build_lookup_map(
    //                 partition,
    //                 stream,
    //                 build_expressions,
    //             ).await?;
    //             Ok(consumer.call(map, records))
    //         },
    //         BuildImplementation::Version6(version6) => {
    //             let (map, records) = version6.build_lookup_map(
    //                 partition,
    //                 stream,
    //                 build_expressions,
    //             ).await?;
    //             Ok(consumer.call(map, records))
    //         },
    //         BuildImplementation::Version7(version7) => {
    //             let (map, records) = version7.build_lookup_map(
    //                 partition,
    //                 stream,
    //                 build_expressions,
    //             ).await?;
    //             Ok(consumer.call(map, records))
    //         },
    //         BuildImplementation::Version8(version8) => {
    //             let (map, records) = version8.build_lookup_map(
    //                 partition,
    //                 stream,
    //                 build_expressions,
    //             ).await?;
    //             Ok(consumer.call(map, records))
    //         },
    //         BuildImplementation::Version9(version9) => {
    //             let (map, records) = version9.build_lookup_map(
    //                 partition,
    //                 stream,
    //                 build_expressions,
    //             ).await?;
    //             Ok(consumer.call(map, records))
    //         },
    //         BuildImplementation::Version10(version10) => {
    //             let (map, records) = version10.build_lookup_map(
    //                 partition,
    //                 stream,
    //                 build_expressions,
    //             ).await?;
    //             Ok(consumer.call(map, records))
    //         },
    //     }
    // }
}

/// Macro to match on BuildImplementation and execute the same code for each version.
///
/// The macro redefines the closure in each branch to allow for different version types.
///
/// # Example
/// ```rust
/// match_build_implementation!(self, |version| {
///     version.build_lookup_map(partition, stream, build_expressions).await
/// })
/// ```
#[macro_export]
macro_rules! match_build_implementation {
    ($func:ident, $value:expr $(, $args:expr)*) => {
        match $value {
            BuildImplementation::Version1(version) => {
                $func(version $(, $args)*).await
            },
            BuildImplementation::Version2(version) => {
                $func(version $(, $args)*).await
            },
            BuildImplementation::Version3(version) => {
                $func(version $(, $args)*).await
            },
            BuildImplementation::Version4(version) => {
                $func(version $(, $args)*).await
            },
            BuildImplementation::Version5(version) => {
                $func(version $(, $args)*).await
            },
            BuildImplementation::Version6(version) => {
                $func(version $(, $args)*).await
            },
            BuildImplementation::Version7(version) => {
                $func(version $(, $args)*).await
            },
            BuildImplementation::Version8(version) => {
                $func(version $(, $args)*).await
            },
            BuildImplementation::Version9(version) => {
                $func(version $(, $args)*).await
            },
            BuildImplementation::Version10(version) => {
                $func(version $(, $args)*).await
            },
        }
    };
}
