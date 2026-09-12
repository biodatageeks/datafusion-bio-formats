fn main() {
    println!("cargo:rerun-if-changed=native");
    if std::env::var_os("CARGO_FEATURE_TEXT_FORMATS").is_some() {
        cc::Build::new()
            .cpp(true)
            .std("c++17")
            .flag_if_supported("/EHsc")
            .warnings(false)
            .include("native/vendor")
            .file("native/cif_bridge.cpp")
            .compile("bio_cif");
    }
}
