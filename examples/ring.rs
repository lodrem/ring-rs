use ring_rs::{Config, Ring};

fn main() {
    let mut r = Ring::new(Config::default());

    r.add("1.1.1.1");
    r.add("2.2.2.2");
    r.add("3.3.3.3");

    println!("{:?}", r.get("1.1.1.1"));
    println!("{:?}", r.get("8.8.8.8"));
    println!("{:?}", r.get("/foo"));
    println!("{:?}", r.get("/bar"));
}
