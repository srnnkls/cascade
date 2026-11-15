fn main() {
    println!("cargo::rustc-check-cfg=cfg(cascade_loom)");
}
