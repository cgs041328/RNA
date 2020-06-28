use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledEngine};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand::rngs::StdRng;
use tempfile::TempDir;

const READ_NUM: u32 = 1000;
const SET_NUM: u32 = 100;
const LEN: u32 = 100000;

pub fn set_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_benchmark");

    group.sample_size(10);
    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let kvs = generate_random_key_values();
                (KvStore::open(temp_dir.path()).unwrap(), kvs)
            },
            |(mut store, kvs)| {
                for (k, v) in kvs {
                    store.set(k, v).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let kvs = generate_random_key_values();
                (SledEngine::open(temp_dir.path()).unwrap(), kvs)
            },
            |(mut store, kvs)| {
                for (k, v) in kvs {
                    store.set(k, v).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

pub fn get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_benchmark");

    group.sample_size(10);
    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let kvs = generate_random_key_values();
                (KvStore::open(temp_dir.path()).unwrap(), kvs)
            },
            |(mut store, kvs)| {
                let mut rng = thread_rng();
                for _ in 0..READ_NUM {
                    let i = rng.gen_range(0, SET_NUM);
                    store.get(kvs[i as usize].0.clone()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let kvs = generate_random_key_values();
                (SledEngine::open(temp_dir.path()).unwrap(), kvs)
            },
            |(mut store, kvs)| {
                let mut rng = thread_rng();
                for _ in 0..READ_NUM {
                    let i = rng.gen_range(0, SET_NUM);
                    store.get(kvs[i as usize].0.clone()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn generate_random_key_values() -> Vec<(String, String)> {
    let mut result = vec![];
    let mut rand_len = StdRng::seed_from_u64(1);

    for _i in 0..SET_NUM {
        let len_k = rand_len.gen_range(1, LEN + 1);
        let k: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(len_k as usize)
            .collect();

        let len_v = rand_len.gen_range(1, LEN + 1);
        let v: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(len_v as usize)
            .collect();
        result.push((k, v));
    }
    result
}

criterion_group!(benches, set_benchmark, get_benchmark);
criterion_main!(benches);
