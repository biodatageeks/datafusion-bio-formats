fn main() {
    println!("cargo:rerun-if-changed=native");
    let mut build = cc::Build::new();
    build
        .cpp(true)
        .std("c++17")
        .warnings(false)
        .define("_USE_MATH_DEFINES", "1")
        .include("native/vendor")
        .file("native/codec_bridge.cpp");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        build.include("native/vendor/windows");
    }
    if std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        // The upstream span header uses std::terminate without <exception>.
        // Force the standard header first, preserving the vendored source bytes.
        build.flag("/EHsc").flag("/FIexception");
    }
    for file in [
        "amino_acid",
        "atom_coordinate",
        "discretizer",
        "foldcomp",
        "nerf",
        "sidechain",
        "torsion_angle",
        "utility",
    ] {
        build.file(format!("native/vendor/{file}.cpp"));
    }
    build.compile("bio_foldcomp");
}
