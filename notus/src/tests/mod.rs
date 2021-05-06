mod common;

use crate::nutos::Notus;
use std::sync::Arc;
use log::{debug, warn};
use anyhow::Error;
use std::alloc::Global;

const N_THREADS: usize = 10;
const N_PER_THREAD: usize = 100;
const N: usize = N_THREADS * N_PER_THREAD; // NB N should be multiple of N_THREADS
const SPACE: usize = N;
#[allow(dead_code)]
const INTENSITY: usize = 10;

fn kv(i: usize) -> Vec<u8> {
    let i = i % SPACE;
    let k = [(i >> 16) as u8, (i >> 8) as u8, i as u8];
    k.to_vec()
}

fn clean_up(dir : &str) {
    fs_extra::dir::remove(format!("./testdir/{}", dir));
}

#[test]
fn monotonic_inserts() {
    clean_up("_test_monotonic_inserts");
    let db = Notus::temp("./testdir/_test_monotonic_inserts").unwrap();

    for len in [1_usize, 16, 32, 1024].iter() {
        for i in 0_usize..*len {
            let mut k = vec![];
            for c in 0_usize..i {
                k.push((c % 256) as u8);
            }
            db.put(k, vec![]).unwrap();
        }


        let count = db.iter().count();
        assert_eq!(count, *len as usize);

        let count2 = db.iter().rev().count();
        assert_eq!(count2, *len as usize);

        db.clear().unwrap();
        //clean_up();
    }


    for len in [1_usize, 16, 32, 1024].iter() {
        for i in (0_usize..*len).rev() {
            let mut k = vec![];
            for c in (0_usize..i).rev() {
                k.push((c % 256) as u8);
            }
            db.put(k, vec![]).unwrap();
        }

        let count3 = db.iter().count();
        assert_eq!(count3, *len as usize);

        let count4 = db.iter().rev().count();
        assert_eq!(count4, *len as usize);

        db.clear().unwrap();
    }
}

#[test]
fn tree_big_keys_iterator() {
    clean_up("_test_tree_big_keys_iterator");
    fn kv(i: usize) -> Vec<u8> {
        let k = [(i >> 16) as u8, (i >> 8) as u8, i as u8];

        let mut base = vec![0; u8::max_value() as usize];
        base.extend_from_slice(&k);
        base
    }

    let t = Notus::temp("./testdir/_test_tree_big_keys_iterator").unwrap();
    for i in 0..N_PER_THREAD {
        let k = kv(i);
        t.put(k.clone(), k).unwrap();
    }

    for (i, (k, v)) in t.iter().map(|res| res.unwrap()).enumerate() {
        let should_be = kv(i);
        assert_eq!(should_be, &*k, "{}", t);
        assert_eq!(should_be, &*v);
    }

    for (i, (k, v)) in t.iter().map(|res| res.unwrap()).enumerate() {
        let should_be = kv(i);
        assert_eq!(should_be, &*k);
        assert_eq!(should_be, &*v);
    }

    let half_way = N_PER_THREAD / 2;
    let half_key = kv(half_way);
    let mut tree_scan = t.range(half_key.clone()..);
    let r1 = tree_scan.next().unwrap().unwrap();
    assert_eq!((r1.0, r1.1), (half_key.clone(), half_key));

    let first_key = kv(0);
    let mut tree_scan = t.range(first_key.clone()..);
    let r2 = tree_scan.next().unwrap().unwrap();
    assert_eq!((r2.0, r2.1), (first_key.clone(), first_key));

    let last_key = kv(N_PER_THREAD - 1);
    let mut tree_scan = t.range(last_key.clone()..);
    let r3 = tree_scan.next().unwrap().unwrap();
    assert_eq!((r3.0, r3.1), (last_key.clone(), last_key));
    assert!(tree_scan.next().is_none());
}

#[test]
fn concurrent_tree_ops() {
    clean_up("_test_concurrent_tree_ops");
    use std::thread;

    common::setup_logger();


    for i in 0..INTENSITY {
        debug!("beginning test {}", i);

        macro_rules! par {
            ($t:ident, $f:expr) => {
                let mut threads = vec![];
                for tn in 0..N_THREADS {
                    let tree = $t.clone();
                    let thread = thread::Builder::new()
                        .name(format!("t(thread: {} test: {})", tn, i))
                        .spawn(move || {
                            println!("thread: {}", tn)
                            for i in
                                (tn * N_PER_THREAD)..((tn + 1) * N_PER_THREAD)
                            {
                                let k = kv(i);
                                $f(&*tree, k);
                            }
                        })
                        .expect("should be able to spawn thread");
                    threads.push(thread);
                }
                while let Some(thread) = threads.pop() {
                    if let Err(e) = thread.join() {
                        panic!("thread failure: {:?}", e);
                    }
                }
            };
        }

        debug!("========== initial sets test {} ==========", i);
        let t = Arc::new(Notus::temp("./testdir/_test_concurrent_tree_ops5").unwrap());
        par! {t, |tree: &Notus, k: Vec<u8>| {
            if let Ok(None) = tree.get(&k) {
                assert!(true)
            }
            else {
                assert!(false)
            }
            //assert_eq!(tree.get(&k), Ok(None));
            tree.put(k.clone(), k.clone()).expect("we should write successfully");
            assert_eq!(tree.get(&k).unwrap(), Some(k.clone().into()),
                "failed to read key {:?} that we just wrote from tree {}",
                k, tree);
        }};

        let n_scanned = t.iter().count();
        if n_scanned != N {
            warn!(
                "WARNING: test {} only had {} keys present \
                 in the DB BEFORE restarting. expected {}",
                i, n_scanned, N,
            );
        }

        drop(t);
        let t = Arc::new(Notus::temp("./testdir/_test_concurrent_tree_ops1").unwrap());

        let n_scanned = t.iter().count();
        if n_scanned != N {
            warn!(
                "WARNING: test {} only had {} keys present \
                 in the DB AFTER restarting. expected {}",
                i, n_scanned, N,
            );
        }

        debug!("========== reading sets in test {} ==========", i);
        par! {t, |tree: &Notus, k: Vec<u8>| {
            if let Some(v) =  tree.get(&k).unwrap() {
                if v != k {
                    panic!("expected key {:?} not found", k);
                }
            } else {
                panic!("could not read key {:?}, which we \
                       just wrote to tree {}", k, tree);
            }
        }};

        drop(t);
        let t = Arc::new(Notus::temp("./testdir/_test_concurrent_tree_ops6").unwrap());



        debug!("========== deleting in test {} ==========", i);
        par! {t, |tree: &Notus, k: Vec<u8>| {
            tree.delete(&k).unwrap();
        }};

        drop(t);
        let t = Arc::new(Notus::temp("./testdir/_test_concurrent_tree_ops9").unwrap());


        par! {t, |tree: &Notus, k: Vec<u8>| {
            if let Ok(None) = tree.get(&k) {
                assert!(true)
            }
            else {
                assert!(false)
            }
            //assert_eq!(tree.get(&k), Ok(None));
        }};
    }
}