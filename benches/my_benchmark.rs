//! Criterion benchmarks for Rod — measuring throughput of core operations.
//!
//! Benchmarks for Rod — measuring throughput of core operations.
//!
//! Benchmarks:
//! 1. **memory_storage get-put** — in-memory storage roundtrip (put → subscribe → recv)
//! 2. **websocket get-put** — cross-node sync over WebSocket (two-node mesh)
//! 3. **JSON parse + verify** — message deserialization for public, content-addressed, and signed puts
//! 4. **redb concurrent read under write load** — read latency while writes hammer the
//!    write actor. Proves the CQRS read/write split keeps reads fast during fsync.
//!
//! Run with: `cargo bench` (requires `--features` for webrtc benchmarks if applicable)

use criterion::async_executor::FuturesExecutor;
use criterion::{Criterion, criterion_group, criterion_main};
use rod::actor::Addr;
use rod::adapters::{MemoryStorage, OutgoingWebsocketManager, RedbStorage, WsServer};
use rod::message::Message;
use rod::{Config, Node};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::runtime::Runtime;
use tokio::time::{Duration, sleep};

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    c.bench_function("memory_storage get-put", |b| {
        rt.block_on(async {
            let mut db = Node::new_with_config(
                Config::default(),
                vec![Box::new(MemoryStorage::new())],
                vec![],
            );
            let counter: AtomicUsize = AtomicUsize::new(0);
            b.to_async(FuturesExecutor).iter(|| {
                let mut db = db.clone();
                let key = counter.fetch_add(1, Ordering::Relaxed).to_string();
                async move {
                    let mut node = db.get("a").get(&key);
                    node.put("hello".into());
                    let mut sub = node.on();
                    sub.recv().await.ok();
                }
            });
            db.stop();
        });
    });

    let mut group = c.benchmark_group("fewer samples");
    group.sample_size(10);

    group.bench_function("websocket get-put", |b| {
        rt.block_on(async {
            //sleep(Duration::from_millis(100)).await;
            let ws_server = Box::new(WsServer::new(Config::default()));
            let ws_client = Box::new(OutgoingWebsocketManager::new(
                Config::default(),
                vec!["http://localhost:4944/ws".to_string()],
            ));
            let mut peer1 = Node::new_with_config(
                Config::default(),
                vec![Box::new(MemoryStorage::new())],
                vec![ws_server],
            );
            //sleep(Duration::from_millis(1000)).await; // let the server start
            let mut peer2 = Node::new_with_config(
                Config::default(),
                vec![Box::new(MemoryStorage::new())],
                vec![ws_client],
            );
            //sleep(Duration::from_millis(1000)).await; // let the ws connect
            let counter: AtomicUsize = AtomicUsize::new(0);
            b.to_async(FuturesExecutor).iter(|| {
                let mut peer1 = peer1.clone();
                let mut peer2 = peer2.clone();
                let key = counter.fetch_add(1, Ordering::Relaxed).to_string();
                async move {
                    peer1.get("a").get(&key).put("hello".into());
                    let mut sub = peer2.get("a").get(&key).on();
                    //sub.recv().await; // TODO enable
                }
            });
            peer1.stop(); // should this be awaitable?
            peer2.stop(); // should this be awaitable?
            sleep(Duration::from_millis(100)).await;
        });
        // https://bheisler.github.io/criterion.rs/book/user_guide/timing_loops.html
    });
    group.finish();

    c.bench_function("parse and verify public space put json", |b| {
        let addr = Addr::noop();
        b.iter(|| {
            Message::try_from(r##"
            [
              {
                "put": {
                  "something": {
                    "_": {
                      "#": "something",
                      ">": {
                        "else": 1653465227430
                      }
                    },
                    "else": "{\"sig\":\"aSEA{\\\"m\\\":{\\\"text\\\":\\\"test post\\\",\\\"time\\\":\\\"2022-05-25T07:53:47.424Z\\\",\\\"type\\\":\\\"post\\\",\\\"author\\\":{\\\"keyID\\\":\\\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\\\"}},\\\"s\\\":\\\"WttDQegXyXILtB1nhNq7Jn69MZ0JD/b1LQrIybQ9UuHn86KvKXg9Lg7+ESmeqSQNaQy7KYvfBEEKbd/ClagQOQ==\\\"}\",\"pubKey\":\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\"}"
                  }
                },
                "#": "yvd2vk4338i"
              }
            ]
            "##, addr.clone(), true).unwrap();
        })
    });

    c.bench_function("parse and verify content addressed put json", |b| {
        let addr = Addr::noop();
        b.iter(|| {
            Message::try_from(r##"
            [
              {
                "put": {
                  "#": {
                    "_": {
                      "#": "#",
                      ">": {
                        "rkHfUdMssQ8Ln9LtiuPTb/ntNxR6HZiVdVsn9DdnKZs=": 1653465227430
                      }
                    },
                    "rkHfUdMssQ8Ln9LtiuPTb/ntNxR6HZiVdVsn9DdnKZs=": "{\"sig\":\"aSEA{\\\"m\\\":{\\\"text\\\":\\\"test post\\\",\\\"time\\\":\\\"2022-05-25T07:53:47.424Z\\\",\\\"type\\\":\\\"post\\\",\\\"author\\\":{\\\"keyID\\\":\\\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\\\"}},\\\"s\\\":\\\"WttDQegXyXILtB1nhNq7Jn69MZ0JD/b1LQrIybQ9UuHn86KvKXg9Lg7+ESmeqSQNaQy7KYvfBEEKbd/ClagQOQ==\\\"}\",\"pubKey\":\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\"}"
                  }
                },
                "#": "yvd2vk4338i"
              }
            ]
            "##, addr.clone(), false).unwrap();
        })
    });

    c.bench_function("parse and verify signed put json", |b| {
        let addr = Addr::noop();
        b.iter(|| {
            Message::try_from(r##"
            {
              "put": {
                "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8": {
                  "_": {
                    "#": "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8",
                    ">": {
                      "profile": 1653463165115
                    }
                  },
                  "profile": "{\":\":{\"#\":\"~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile\"},\"~\":\"JW+tFHHVBaY+zm/uzUoGVlogvXXQIA3vFNT0f0uX6tnnPGrRevDWzEmnVYy+ChxS6AJi5THiPyOc2HorIIM5wg==\"}"
                },
                "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile": {
                  "_": {
                    ">": {
                      "name": 1653463165115
                    },
                    "#": "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile"
                  },
                  "name": "{\":\":\"Arja Koriseva\",\"~\":\"KCq2D/T0mMenizxiVMso8FO5JIv9ZJLA0Q67DFa9qssPSKCmmieC1Nl5+nRpOX29C6A2/kLaJgphN/X7kUQjww==\"}"
                }
              },
              "#": "issWkzotF"
            }
            "##, addr.clone(), false).unwrap();
        })
    });

    // Benchmark: concurrent reads under write load using redb.
    //
    // Measures read latency (once()) while a background task continuously
    // writes to the same database. With the CQRS split, reads go through
    // the read actor and are not blocked by the write actor's spawn_blocking
    // fsync. Without the split, reads would queue behind writes.
    let mut group = c.benchmark_group("concurrent read under write");
    group.sample_size(10);

    group.bench_function("redb read during write", |b| {
        rt.block_on(async {
            let temp_path =
                std::env::temp_dir().join(format!("rod-bench-{}.redb", std::process::id()));
            let _ = std::fs::remove_file(&temp_path);

            let config = Config::default();
            let mut db = Node::new_with_config(
                config.clone(),
                vec![Box::new(RedbStorage::new_with_config(
                    config,
                    temp_path.to_string_lossy().as_ref(),
                    None,
                ))],
                vec![],
            );

            // Seed a key so reads have something to find.
            db.get("bench_key").put("bench_value".into());
            sleep(Duration::from_millis(100)).await;

            // Background writer: continuously puts to a different key.
            let mut writer_db = db.clone();
            let write_counter = Arc::new(AtomicUsize::new(0));
            let writer_counter_clone = write_counter.clone();
            let stop_writer = Arc::new(AtomicBool::new(false));
            let stop_clone = stop_writer.clone();
            let writer_handle = tokio::spawn(async move {
                loop {
                    if stop_clone.load(Ordering::Relaxed) {
                        break;
                    }
                    let n = writer_counter_clone.fetch_add(1, Ordering::Relaxed);
                    writer_db.get("write_load").put(format!("val-{n}").into());
                    // Small yield to avoid saturating the channel.
                    tokio::task::yield_now().await;
                }
            });

            let read_counter = AtomicUsize::new(0);
            b.to_async(FuturesExecutor).iter(|| {
                let mut db = db.clone();
                async move {
                    let _ = db.get("bench_key").once(Some(Duration::from_secs(2))).await;
                }
            });

            // Stop the background writer.
            stop_writer.store(true, Ordering::Relaxed);
            let _ = writer_handle.await;
            db.stop();
            sleep(Duration::from_millis(200)).await;
            let _ = std::fs::remove_file(&temp_path);
        });
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
