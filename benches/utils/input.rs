use rand::prelude::SliceRandom;

pub fn limited_random(num: usize) -> impl Fn() -> u64 {
    let choices = (0..num).into_iter().map(|x| rand::random()).collect::<Vec<u64>>();
    move || *choices.choose(&mut rand::thread_rng()).unwrap()
}

pub fn make_input(parallelism: usize, batches: usize, batch_size: usize, mut generator: impl FnMut() -> u64) -> Vec<Vec<Vec<u64>>> {
    (0..parallelism).into_iter()
        .map(|_thread_index| {
            (0..batches).into_iter()
                .map(|_batch_index| {
                    // let min = thread_index * batch_size + batch_index * parallelism * batch_size;
                    make_vec(batch_size, || generator())
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

pub fn make_vec(batch_size: usize, mut generator: impl FnMut() -> u64 + Sized) -> Vec<u64> {
    (0..batch_size).into_iter().map(|_| generator()).collect::<Vec<u64>>()
}
