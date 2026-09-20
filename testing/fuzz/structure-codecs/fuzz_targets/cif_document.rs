#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    structure_codecs_fuzz::cif_document(data);
});
