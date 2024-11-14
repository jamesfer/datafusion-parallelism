use datafusion::arrow::array::{Array, ArrayRef, PrimitiveArray, RecordBatch, UInt32Array, UInt32BufferBuilder, UInt32Builder, UInt64Array, UInt64BufferBuilder, UInt64Builder};
use datafusion::arrow::error::ArrowError;
use ahash::RandomState;
use datafusion::arrow;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_common::DataFusionError;
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::physical_expr::PhysicalExpr;
use crate::utils::index_lookup::IndexLookup;

pub fn calculate_hash(values: &Vec<ArrayRef>) -> Result<Vec<u64>, ArrowError> {
    let capacity = values.get(0).map(|array| array.len()).unwrap_or(0);
    let mut probe_hashes = vec![0; capacity];
    datafusion_common::hash_utils::create_hashes(&values, &RandomState::with_seed(0), &mut probe_hashes)?;
    Ok(probe_hashes)
}

pub fn evaluate_expressions(expressions: &Vec<PhysicalExprRef>, batch: &RecordBatch) -> Result<Vec<ArrayRef>, DataFusionError> {
    expressions.iter()
        .map(|expression| expression.evaluate(batch)?.into_array(batch.num_rows()))
        .collect::<datafusion_common::Result<Vec<_>>>()
}

pub struct ProbeBuildIndices {
    pub probe_indices: UInt32Array,
    pub build_indices: UInt64Array,
}

pub fn get_matching_indices<Lookup>(probe_hashes: &Vec<u64>, join_map: &Lookup) -> ProbeBuildIndices
    where Lookup: IndexLookup<u64>
{
    let mut probe_indices = UInt32BufferBuilder::new(0);
    let mut build_indices = UInt64BufferBuilder::new(0);
    for (probe_index, hash) in probe_hashes.iter().enumerate() {
        // Append all the matching build indices
        let mut matching_count = 0;
        for matching_build_index in join_map.get_iter(hash) {
            build_indices.append(matching_build_index as u64);
            matching_count += 1;
        }
        probe_indices.append_n(matching_count, probe_index as u32);
    }

    let probe_indices = PrimitiveArray::new(ScalarBuffer::from(probe_indices.finish()), None);
    let build_indices = PrimitiveArray::new(ScalarBuffer::from(build_indices.finish()), None);
    ProbeBuildIndices { probe_indices, build_indices }
}

pub fn get_matching_indices_with_probe<Lookup>(probe_hashes: &Vec<u64>, join_map: &Lookup) -> ProbeBuildIndices
    where Lookup: IndexLookup<u64>
{
    // TODO build indices actually need to be a 64 buffer
    let mut probe_indices = UInt32BufferBuilder::new(0);
    let mut build_indices = UInt64Builder::new();
    for (probe_index, hash) in probe_hashes.iter().enumerate() {
        let probe_index = probe_index as u32;

        // Append all the matching build indices
        let mut matching_count = 0;
        for matching_build_index in join_map.get_iter(hash) {
            build_indices.append_value(matching_build_index as u64);
            matching_count += 1;
        }

        match matching_count {
            // If none matched, append null
            0 => {
                probe_indices.append(probe_index);
                build_indices.append_null();
            },
            // Fill in the probe side to match the length of the build side
            n => {
                probe_indices.append_n(n, probe_index);
            },
        }
    }

    let probe_indices = PrimitiveArray::new(ScalarBuffer::from(probe_indices.finish()), None);
    let build_indices = build_indices.finish();
    ProbeBuildIndices { probe_indices, build_indices }
}

pub fn take_multiple_record_batch(vec: Vec<(&RecordBatch, &dyn Array)>) -> Result<Vec<ArrayRef>, ArrowError> {
    vec.into_iter()
        .flat_map(|(record_batch, indices)| {
            record_batch
                .columns()
                .iter()
                .map(|column| arrow::compute::take(column, indices, None))
        })
        .collect()
}
