pub struct KvStore {}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {}
    }

    pub fn set(&mut self, key: String, value: String) {}
    pub fn get(&mut self, key: String) -> Option<String> {
        panic!()
    }
    pub fn remove(&mut self, key: String) {}
}
