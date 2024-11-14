use datafusion::arrow::array::{Int32Array, Int32BufferBuilder, PrimitiveArray, StringArray};
use datafusion::arrow::datatypes::Int32Type;
use num_traits::pow::Pow;


pub fn make_int_array_with_shift(min: i32, max: i32, shift: i32) -> Int32Array {
    let ids = ((min + shift)..(max + shift)).into_iter().collect::<Vec<_>>();
    Int32Array::from(ids)
}

pub fn make_int_array_from_range(min: i32, max: i32) -> Int32Array {
    make_int_array_with_shift(min, max, 0)
}

pub fn make_exponential_int_array(min: i32, max: i32) -> Int32Array {
    let base = 16f32;
    let diff = max - min;
    make_int_array_it((0..diff).into_iter().map(|n| {
        let x = n as f32 / diff as f32;
        let y = (base.pow(x) - 1f32) / (base - 1f32);
        min + (y * diff as f32) as i32
    }))
}

pub fn make_int_array_it<I: Iterator<Item=i32>>(iter: I) -> Int32Array {
    Int32Array::from(iter.collect::<Vec<_>>())
}

pub fn make_int_array<T>(iter: impl IntoIterator<Item=T>) -> Int32Array
where PrimitiveArray<Int32Type>: From<Vec<T>>
{
    Int32Array::from(iter.into_iter().collect::<Vec<_>>())
}

pub fn make_int_array_it_nullable(iter: impl IntoIterator<Item=Option<i32>>) -> Int32Array {
    Int32Array::from(iter.into_iter().collect::<Vec<_>>())
}

pub fn make_string_constant_array(value: String, count: i32) -> StringArray {
    StringArray::from((0..count).map(|_| value.clone()).collect::<Vec<_>>())
}

pub fn make_string_array(iter: impl IntoIterator<Item=String>) -> StringArray {
    StringArray::from(iter.into_iter().collect::<Vec<_>>())
}

pub fn make_string_array_nullable(iter: impl IntoIterator<Item=Option<String>>) -> StringArray {
    StringArray::from(iter.into_iter().collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use crate::api_utils::make_exponential_int_array;

    #[test]
    fn x() {
        let vec = make_exponential_int_array(0, 10).iter().collect::<Vec<_>>();
        assert_eq!(vec, vec![
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(1),
            Some(2),
            Some(2),
            Some(3),
            Some(5),
            Some(7),
        ])
    }
}
