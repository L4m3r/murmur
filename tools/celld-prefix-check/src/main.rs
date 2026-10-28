mod baseline;
mod patched;
mod asyncrt {
    pub fn wall_ms() -> i64 { 123456789 }
    pub async fn blocking<F, R>(f: F) -> Result<R, tokio::task::JoinError>
    where F: FnOnce() -> R + Send + 'static, R: Send + 'static {
        tokio::task::spawn_blocking(f).await
    }
}
use object_store::{ObjectStore, path::Path, list::{PaginatedListStore, PaginatedListOptions}};
use futures_util::TryStreamExt;
#[tokio::main]
async fn main() {
    let db = std::env::args().nth(1).expect("fresh synthetic db path");
    assert!(!std::path::Path::new(&db).exists());
    let a = baseline::LocalStore::open(&db).unwrap();
    let b = patched::LocalStore::open(&db).unwrap();
    let prefixes = ["", "a", "ab", "a%", "a_", "a*", "a?", "a[", "é", "e\u{301}", "中", "\u{d7ff}", "\u{e000}", "\u{10ffff}", "missing", "a/b", "a/", "é/", "/a/", "/"];
    let mut keys = std::collections::BTreeSet::new();
    for p in prefixes { for suffix in ["", "/x", "/x/y", "/x/z", "/é", "/中/y", "0", "0/x", ".", ".x", "-x"] {
        let key = format!("{p}{suffix}");
        if let Ok(path) = Path::parse(&key) { keys.insert(path); }
        keys.insert(Path::from(key.as_str()));
    }}
    for i in 0..500 { keys.insert(Path::from(format!("unrelated/{i:05}/blob"))); }
    for (index, key) in keys.iter().enumerate() {
        let mut attributes = object_store::Attributes::new();
        attributes.insert(object_store::Attribute::ContentType, "application/octet-stream".into());
        let body = bytes::Bytes::from(vec![42; index % 4096]);
        a.put_opts(key, body.into(), object_store::PutOptions { attributes, ..Default::default() }).await.unwrap();
    }
    let mut cases = 0usize;
    for p in std::iter::once(None).chain(prefixes.iter().map(|p| Some(Path::parse(*p).unwrap()))).chain(prefixes.iter().map(|p| Some(Path::from(*p)))) {
        let x: Vec<_> = a.list(p.as_ref()).try_collect().await.unwrap();
        let y: Vec<_> = b.list(p.as_ref()).try_collect().await.unwrap();
        assert_eq!(format!("{x:?}"), format!("{y:?}"), "list {p:?}");
        let x = a.list_with_delimiter(p.as_ref()).await.unwrap();
        let y = b.list_with_delimiter(p.as_ref()).await.unwrap();
        assert_eq!(format!("{x:?}"), format!("{y:?}"), "delimiter {p:?}");
        cases += 2;
        for offset in ["", "a", "a/x", "é", "中"] {
            let offset = Path::parse(offset).unwrap();
            let x: Vec<_> = a.list_with_offset(p.as_ref(), &offset).try_collect().await.unwrap();
            let y: Vec<_> = b.list_with_offset(p.as_ref(), &offset).try_collect().await.unwrap();
            assert_eq!(format!("{x:?}"), format!("{y:?}"), "offset {p:?} {offset:?}");
            cases += 1;
        }
    }
    for p in prefixes { for delimiter in [None, Some("/"), Some("x"), Some("é"), Some("")] { for limit in [0, 1, 2, 7, 1000] { for offset in [None, Some("a"), Some("a/x"), Some("é")] {
        let mut token = None;
        for _ in 0..1000 {
            let options = PaginatedListOptions { max_keys: Some(limit), delimiter: delimiter.map(|x| x.to_owned().into()), offset: offset.map(str::to_owned), page_token: token.clone(), ..Default::default() };
            let x = a.list_paginated(Some(p), options.clone()).await.unwrap();
            let y = b.list_paginated(Some(p), options).await.unwrap();
            assert_eq!(format!("{x:?}"), format!("{y:?}"), "page {p:?} {delimiter:?} {limit} {offset:?}");
            cases += 1;
            if x.page_token.is_none() { break; }
            assert_ne!(token, x.page_token);
            token = x.page_token;
        }
    }}}}
    let conn = rusqlite::Connection::open(&db).unwrap();
    let plan: String = conn.query_row("EXPLAIN QUERY PLAN SELECT key, CAST(X'' AS BLOB), etag, modified_ms, attributes, length(body) FROM objects WHERE key >= ?1 AND key < ?2 ORDER BY key", ["a/", "a0"], |r| r.get(3)).unwrap();
    assert!(plan.contains("SEARCH objects USING INDEX") && plan.contains("key>? AND key<?"), "{plan}");
    println!("PASS {cases} exact debug-metadata/list/pagination comparisons over {} synthetic keys; plan={plan}", keys.len());
}
