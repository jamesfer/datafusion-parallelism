pub trait IndexLookup<T> {
    type It<'a>: Iterator<Item=usize> + 'a
        where Self: 'a, T: 'a;

    fn get_iter<'a>(&'a self, key: &'a T) -> Self::It<'a>;
}
