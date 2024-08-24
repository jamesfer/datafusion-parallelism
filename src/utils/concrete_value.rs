use crate::utils::index_lookup::IndexLookup;

// pub trait Callable<O: ?Sized, R> {
//     fn call<T: O>(value: T) -> R;
// }

// pub trait Test2Receiver<R> {
//     fn call<T, A>(value: T) -> R
//         where T: Test2<A=A>;
// }
//
// trait Test1 {
//
// }
//
// trait Test2 {
//     type A;
// }
//
// fn a() {
//     let c: impl Callable<dyn Test1, i32>;
//     let c: impl Callable<dyn Test2, i32>;
// }

// pub trait ConcreteValue {
//     fn consume<R>(self, f: dyn Callable<dyn IndexLookup<u64>, R>)
// }
