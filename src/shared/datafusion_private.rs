use std::ops::Range;
use datafusion::arrow::array::{downcast_array, Array, ArrayRef, ArrowPrimitiveType, BooleanArray, BooleanBufferBuilder, NativeAdapter, PrimitiveArray, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder};
use datafusion::arrow::compute::kernels::cmp::{eq, not_distinct};
use datafusion::arrow::compute::{and, take, FilterBuilder};
use datafusion::arrow::datatypes::{ArrowNativeType, UInt32Type, UInt64Type};
use datafusion::arrow::error::ArrowError;
use datafusion::logical_expr::Operator;
use datafusion_common::DataFusionError;
use datafusion_physical_expr_common::datum::compare_op_for_nested;

// Methods are copied from DataFusion library, as they are not publicly accessible.

// version of eq_dyn supporting equality on null arrays
fn eq_dyn_null(
    left: &dyn Array,
    right: &dyn Array,
    null_equals_null: bool,
) -> Result<BooleanArray, ArrowError> {
    // Nested datatypes cannot use the underlying not_distinct/eq function and must use a special
    // implementation
    // <https://github.com/apache/datafusion/issues/10749>
    if left.data_type().is_nested() {
        let op = if null_equals_null {
            Operator::IsNotDistinctFrom
        } else {
            Operator::Eq
        };
        return Ok(compare_op_for_nested(op, &left, &right)?);
    }
    match (left.data_type(), right.data_type()) {
        _ if null_equals_null => not_distinct(&left, &right),
        _ => eq(&left, &right),
    }
}

pub fn equal_rows_arr(
    indices_left: &UInt64Array,
    indices_right: &UInt32Array,
    left_arrays: &[ArrayRef],
    right_arrays: &[ArrayRef],
    null_equals_null: bool,
) -> Result<(UInt64Array, UInt32Array), DataFusionError> {
    let mut iter = left_arrays.iter().zip(right_arrays.iter());

    let (first_left, first_right) = iter.next().ok_or_else(|| {
        DataFusionError::Internal(
            "At least one array should be provided for both left and right".to_string(),
        )
    })?;

    let arr_left = take(first_left.as_ref(), indices_left, None)?;
    let arr_right = take(first_right.as_ref(), indices_right, None)?;

    let mut equal: BooleanArray = eq_dyn_null(&arr_left, &arr_right, null_equals_null)?;

    // Use map and try_fold to iterate over the remaining pairs of arrays.
    // In each iteration, take is used on the pair of arrays and their equality is determined.
    // The results are then folded (combined) using the and function to get a final equality result.
    equal = iter
        .map(|(left, right)| {
            let arr_left = take(left.as_ref(), indices_left, None)?;
            let arr_right = take(right.as_ref(), indices_right, None)?;
            eq_dyn_null(arr_left.as_ref(), arr_right.as_ref(), null_equals_null)
        })
        .try_fold(equal, |acc, equal2| and(&acc, &equal2?))?;

    let filter_builder = FilterBuilder::new(&equal).optimize().build();

    let left_filtered = filter_builder.filter(indices_left)?;
    let right_filtered = filter_builder.filter(indices_right)?;

    Ok((
        downcast_array(left_filtered.as_ref()),
        downcast_array(right_filtered.as_ref()),
    ))
}

// Copied from anti join in the DataFusion library

/// Returns `range` indices which are not present in `input_indices`
pub fn get_anti_indices<T: ArrowPrimitiveType>(
    range: Range<usize>,
    input_indices: &PrimitiveArray<T>,
) -> PrimitiveArray<T>
where
    NativeAdapter<T>: From<<T as ArrowPrimitiveType>::Native>,
{
    let mut bitmap = BooleanBufferBuilder::new(range.len());
    bitmap.append_n(range.len(), false);
    input_indices
        .iter()
        .flatten()
        .map(|v| v.as_usize())
        .filter(|v| range.contains(v))
        .for_each(|v| {
            bitmap.set_bit(v - range.start, true);
        });

    let offset = range.start;

    // get the anti index
    (range)
        .filter_map(|idx| {
            (!bitmap.get_bit(idx - offset)).then_some(T::Native::from_usize(idx))
        })
        .collect()
}

/// Returns intersection of `range` and `input_indices` omitting duplicates
pub fn get_semi_indices<T: ArrowPrimitiveType>(
    range: Range<usize>,
    input_indices: &PrimitiveArray<T>,
) -> PrimitiveArray<T>
where
    NativeAdapter<T>: From<<T as ArrowPrimitiveType>::Native>,
{
    let mut bitmap = BooleanBufferBuilder::new(range.len());
    bitmap.append_n(range.len(), false);
    input_indices
        .iter()
        .flatten()
        .map(|v| v.as_usize())
        .filter(|v| range.contains(v))
        .for_each(|v| {
            bitmap.set_bit(v - range.start, true);
        });

    let offset = range.start;

    // get the semi index
    (range)
        .filter_map(|idx| {
            (bitmap.get_bit(idx - offset)).then_some(T::Native::from_usize(idx))
        })
        .collect()
}

/// Appends right indices to left indices based on the specified order mode.
///
/// The function operates in two modes:
/// 1. If `preserve_order_for_right` is true, probe matched and unmatched indices
///    are inserted in order using the `append_probe_indices_in_order()` method.
/// 2. Otherwise, unmatched probe indices are simply appended after matched ones.
///
/// # Parameters
/// - `left_indices`: UInt64Array of left indices.
/// - `right_indices`: UInt32Array of right indices.
/// - `adjust_range`: Range to adjust the right indices.
/// - `preserve_order_for_right`: Boolean flag to determine the mode of operation.
///
/// # Returns
/// A tuple of updated `UInt64Array` and `UInt32Array`.
pub(crate) fn append_right_indices(
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    adjust_range: Range<usize>,
    preserve_order_for_right: bool,
) -> (UInt64Array, UInt32Array) {
    if preserve_order_for_right {
        append_probe_indices_in_order(left_indices, right_indices, adjust_range)
    } else {
        let right_unmatched_indices = get_anti_indices(adjust_range, &right_indices);

        if right_unmatched_indices.is_empty() {
            (left_indices, right_indices)
        } else {
            let unmatched_size = right_unmatched_indices.len();
            // the new left indices: left_indices + null array
            // the new right indices: right_indices + right_unmatched_indices
            let new_left_indices = left_indices
                .iter()
                .chain(std::iter::repeat(None).take(unmatched_size))
                .collect();
            let new_right_indices = right_indices
                .iter()
                .chain(right_unmatched_indices.iter())
                .collect();
            (new_left_indices, new_right_indices)
        }
    }
}

/// Appends probe indices in order by considering the given build indices.
///
/// This function constructs new build and probe indices by iterating through
/// the provided indices, and appends any missing values between previous and
/// current probe index with a corresponding null build index.
///
/// # Parameters
///
/// - `build_indices`: `PrimitiveArray` of `UInt64Type` containing build indices.
/// - `probe_indices`: `PrimitiveArray` of `UInt32Type` containing probe indices.
/// - `range`: The range of indices to consider.
///
/// # Returns
///
/// A tuple of two arrays:
/// - A `PrimitiveArray` of `UInt64Type` with the newly constructed build indices.
/// - A `PrimitiveArray` of `UInt32Type` with the newly constructed probe indices.
fn append_probe_indices_in_order(
    build_indices: PrimitiveArray<UInt64Type>,
    probe_indices: PrimitiveArray<UInt32Type>,
    range: Range<usize>,
) -> (PrimitiveArray<UInt64Type>, PrimitiveArray<UInt32Type>) {
    // Builders for new indices:
    let mut new_build_indices = UInt64Builder::new();
    let mut new_probe_indices = UInt32Builder::new();
    // Set previous index as the start index for the initial loop:
    let mut prev_index = range.start as u32;
    // Zip the two iterators.
    debug_assert!(build_indices.len() == probe_indices.len());
    for (build_index, probe_index) in build_indices
        .values()
        .into_iter()
        .zip(probe_indices.values().into_iter())
    {
        // Append values between previous and current probe index with null build index:
        for value in prev_index..*probe_index {
            new_probe_indices.append_value(value);
            new_build_indices.append_null();
        }
        // Append current indices:
        new_probe_indices.append_value(*probe_index);
        new_build_indices.append_value(*build_index);
        // Set current probe index as previous for the next iteration:
        prev_index = probe_index + 1;
    }
    // Append remaining probe indices after the last valid probe index with null build index.
    for value in prev_index..range.end as u32 {
        new_probe_indices.append_value(value);
        new_build_indices.append_null();
    }
    // Build arrays and return:
    (new_build_indices.finish(), new_probe_indices.finish())
}
