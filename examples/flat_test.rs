// Standalone: does Rod handle flat keys?
#[tokio::main]
async fn main() {
    use rod::Node;
    use rod::Value;
    
    // FLAT
    let mut db = Node::new();
    let mut flat = db.get("a");
    flat.put("flat_val".into());
    let flat_once = flat.once(None).await;
    println!("FLAT once(): {:?}", flat_once);
    
    let mut db2 = Node::new();
    let mut flat2 = db2.get("a");
    flat2.put("flat2".into());
    let mut sub = flat2.on();
    let flat_recv = sub.recv().await;
    println!("FLAT recv(): {:?}", flat_recv);
    
    // NESTED (same pattern as Rod tests)
    let mut db3 = Node::new();
    let mut nested = db3.get("x").get("y");
    nested.put("nested_val".into());
    let mut sub3 = nested.on();
    let nested_recv = sub3.recv().await;
    println!("NESTED recv(): {:?}", nested_recv);
    
    let mut db4 = Node::new();
    let mut nested2 = db4.get("p").get("q");
    nested2.put("nested_once".into());
    let nested_once = nested2.once(None).await;
    println!("NESTED once(): {:?}", nested_once);
}
