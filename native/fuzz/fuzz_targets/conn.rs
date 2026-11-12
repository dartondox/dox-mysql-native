#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| dox_mysql_native::fuzz::conn(data));
