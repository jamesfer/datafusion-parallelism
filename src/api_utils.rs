use datafusion::arrow::array::{Int32Array, StringArray};

pub fn make_int_array(min: i32, max: i32, shift: i32) -> Int32Array {
    let range = max - min;
    let ids = (0..range).into_iter()
        .map(|value| (value + shift) % range + min)
        .collect::<Vec<_>>();
    Int32Array::from(ids)
}

pub fn make_string_constant_array(value: String, count: i32) -> StringArray {
    StringArray::from((0..count).map(|_| value.clone()).collect::<Vec<_>>())
}
