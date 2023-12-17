use obi::{OBIDecode, OBIEncode, OBISchema};
use owasm_kit::{execute_entry_point, prepare_entry_point, oei, ext};

#[derive(OBIDecode, OBISchema)]
struct Input {
    repeat: u64,
}

#[derive(OBIEncode, OBISchema)]
struct Output {
    response: String,
}

const DATA_SOURCE_ID: i64 = 327;
const EXTERNAL_ID: i64 = 0;

#[no_mangle]
fn prepare_impl(_input: Input) {
    oei::ask_external_data(
        EXTERNAL_ID,
        DATA_SOURCE_ID,
        b"",
    )
}

#[no_mangle]
fn execute_impl(input: Input) -> Output {
    let raw_result = ext::load_input::<String>(EXTERNAL_ID);
    let result: Vec<String> = raw_result.collect();
    let majority_result: String = ext::stats::majority(result).unwrap();
    Output {
        response: majority_result.repeat(input.repeat as usize),
    }
}

prepare_entry_point!(prepare_impl);
execute_entry_point!(execute_impl);