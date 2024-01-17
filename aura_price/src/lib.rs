use anyhow::{bail, Result};
use num::{FromPrimitive, Integer};
use std::collections::HashMap;
use std::iter::zip;

use obi::{OBIDecode, OBIEncode, OBISchema};
use owasm_kit::{execute_entry_point, ext, oei, prepare_entry_point};
use phf::phf_map;

const MULTIPLIER: u64 = 1000000000;
const DATA_SOURCE_COUNT: usize = 11;

#[derive(OBIDecode, OBISchema)]
struct Input {
    symbols: Vec<String>,
    minimum_source_count: u8,
}

#[derive(PartialEq, Debug)]
enum ResponseCode {
    Success,
    SymbolNotSupported,
    NotEnoughSources,
    ConversionError,
    Unknown = 127,
}

#[derive(OBIEncode, OBISchema, PartialEq, Debug)]
struct Response {
    symbol: String,
    response_code: u8,
    rate: u64,
}

impl Response {
    fn new(symbol: String, response_code: ResponseCode, rate: u64) -> Self {
        Response {
            symbol,
            response_code: response_code as u8,
            rate,
        }
    }
}

#[derive(OBIEncode, OBISchema, PartialEq, Debug)]
struct Output {
    responses: Vec<Response>,
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum DataSources {
    BINANCE = 54,
    COINBASEPRO = 73,
    COINGECKO = 74,
    COINMARKETCAP = 72,
    CRYPTOCOMPARE = 71,
    HITBTC = 76,
    HUOBIPRO = 59,
    KRAKEN = 58,
    OKX = 56,
}

static SYMBOLS: phf::Map<&'static str, &'static [DataSources]> = phf_map! {
    "ETH" => &[DataSources::BINANCE, DataSources::COINBASEPRO, DataSources::COINGECKO, DataSources::COINMARKETCAP, DataSources::CRYPTOCOMPARE, DataSources::HITBTC, DataSources::HUOBIPRO, DataSources::KRAKEN, DataSources::OKX],
    "AURA" => &[DataSources::COINGECKO, DataSources::COINMARKETCAP],
};

/// Returns a HashMap mapping the data source id to its supported symbols
fn get_symbols_for_data_sources(symbols: &[String]) -> HashMap<i64, Vec<String>> {
    symbols.iter().fold(
        HashMap::with_capacity(DATA_SOURCE_COUNT),
        |mut acc, symbol| {
            if let Some(data_sources) = SYMBOLS.get(symbol.as_str()) {
                for ds in *data_sources {
                    acc.entry(*ds as i64)
                        .and_modify(|e| {
                            e.push(symbol.clone());
                        })
                        .or_insert(vec![symbol.clone()]);
                }
            }
            acc
        },
    )
}

/// Parses the individual values to assure its value is usable
fn validate_value(v: &str) -> Result<Option<f64>> {
    if v == "-" {
        Ok(None)
    } else {
        let val = v.parse::<f64>()?;
        if val < 0f64 {
            bail!("Invalid value")
        }
        Ok(Some(val))
    }
}

/// Validates and parses the a validator's data source output
fn validate_and_parse_output(ds_output: &str, length: usize) -> Result<Vec<Option<f64>>> {
    let parsed_output = ds_output
        .split(",")
        .map(|v| validate_value(v.trim()))
        .collect::<Result<Vec<Option<f64>>>>()?;

    // If the length of the parsed output is not equal to the expected length, raise an error
    if parsed_output.len() != length {
        bail!("Mismatched length");
    }

    Ok(parsed_output)
}

/// Gets the minimum successful response required given the minimum request count
fn get_minimum_response_count(min_count: i64) -> usize {
    if min_count.is_even() {
        ((min_count + 2) / 2) as usize
    } else {
        ((min_count + 1) / 2) as usize
    }
}

/// Filters and medianizes the parsed data source output
fn filter_and_medianize(
    rates: Vec<Vec<Option<f64>>>,
    length: usize,
    min_response: usize,
) -> Vec<Option<f64>> {
    (0..length)
        .map(|i| {
            let symbol_rates = rates.iter().filter_map(|o| o[i]).collect::<Vec<f64>>();
            if symbol_rates.len() < min_response {
                None
            } else {
                ext::stats::median_by(symbol_rates, ext::cmp::fcmp)
            }
        })
        .collect::<Vec<Option<f64>>>()
}

/// Aggregates the data sources outputs to either a result or error
fn aggregate_value(rates: &[f64], minimum_source_count: usize) -> Result<u64, ResponseCode> {
    if rates.len() < minimum_source_count {
        Err(ResponseCode::NotEnoughSources)
    } else {
        if let Some(price) = ext::stats::median_by(rates.to_owned(), ext::cmp::fcmp) {
            if let Some(mul_price) = u64::from_f64(price * MULTIPLIER as f64) {
                Ok(mul_price)
            } else {
                Err(ResponseCode::ConversionError)
            }
        } else {
            Err(ResponseCode::Unknown)
        }
    }
}

/// Gets the oracle script responses
fn get_responses(
    symbols: &[String],
    symbol_prices: HashMap<String, Vec<f64>>,
    minimum_source_count: usize,
) -> Vec<Response> {
    symbols
        .iter()
        .map(|symbol| {
            if let Some(prices) = symbol_prices.get(symbol) {
                match aggregate_value(&prices, minimum_source_count) {
                    Ok(rate) => Response::new(symbol.clone(), ResponseCode::Success, rate),
                    Err(code) => Response::new(symbol.clone(), code, 0),
                }
            } else {
                Response::new(symbol.clone(), ResponseCode::SymbolNotSupported, 0)
            }
        })
        .collect()
}

fn prepare_impl(input: Input) {
    for (id, symbols) in get_symbols_for_data_sources(&input.symbols) {
        oei::ask_external_data(id, id, symbols.join(" ").as_bytes())
    }
}

fn execute_impl(input: Input) -> Output {
    // HashMap containing all symbols and a vector of their prices from each data source
    let mut symbol_prices: HashMap<String, Vec<f64>> = HashMap::with_capacity(input.symbols.len());

    // Gets the minimum required response count
    let min_resp_count = get_minimum_response_count(oei::get_min_count());

    for (id, symbols) in get_symbols_for_data_sources(&input.symbols) {
        // Parses the validator's responses from a raw string
        let ds_outputs = ext::load_input::<String>(id)
            .filter_map(|r| validate_and_parse_output(&r, symbols.len()).ok())
            .collect::<Vec<Vec<Option<f64>>>>();

        // Gets data source median rates
        let median_rates = filter_and_medianize(ds_outputs, symbols.len(), min_resp_count);

        // Saves symbol rates
        for (symbol, opt_rate) in zip(symbols, median_rates) {
            if let Some(rate) = opt_rate {
                symbol_prices
                    .entry(symbol)
                    .and_modify(|e| e.push(rate))
                    .or_insert(vec![rate]);
            }
        }
    }

    Output {
        responses: get_responses(
            &input.symbols,
            symbol_prices,
            input.minimum_source_count as usize,
        ),
    }
}

prepare_entry_point!(prepare_impl);
execute_entry_point!(execute_impl);
