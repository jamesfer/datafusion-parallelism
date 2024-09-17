use datafusion::arrow::array::{Int32Array, StringArray};

pub fn make_int_array(min: i32, max: i32, shift: i32) -> Int32Array {
    let ids = ((min + shift)..(max + shift)).into_iter().collect::<Vec<_>>();
    Int32Array::from(ids)
}

pub fn make_string_constant_array(value: String, count: i32) -> StringArray {
    StringArray::from((0..count).map(|_| value.clone()).collect::<Vec<_>>())
}

pub fn make_string_array<I: Iterator<Item=String>>(iter: I) -> StringArray {
    StringArray::from(iter.collect::<Vec<_>>())
}
