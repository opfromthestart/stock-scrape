use std::{sync::{Arc, RwLock}, time::Duration};

use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};

//"https://query1.finance.yahoo.com/v8/finance/chart/HOOD?formatted=true&crumb=MJDLmJKn%2Fyt&lang=en-US&region=US&includeAdjustedClose=true&interval=1d&period1=0&period2=1677801600&events=capitalGain%7Cdiv%7Csplit&useYfid=true&corsDomain=finance.yahoo.com"

const YAHOO1 : &str = "https://query1.finance.yahoo.com/v8/finance/chart/";
const YAHOO2 : &str = "?formatted=true&crumb=MJDLmJKn%2Fyt&lang=en-US&region=US&includeAdjustedClose=true&interval=1d&period1=0&period2=2077801600&events=capitalGain%7Cdiv%7Csplit&useYfid=true&corsDomain=finance.yahoo.com";

#[derive(Deserialize)]
struct ChartResp {
    chart: ResultResp,
}

#[derive(Deserialize)]
struct ResultResp {
    result: Vec<TickData>,
}

impl ChartResp {
    fn to_tuples(&self) -> Option<Vec<Point>> {
        let r = &self.chart.result.get(0)?;
        let q = &r.indicators.quote.get(0)?;
        Some(r.timestamp.iter()
        .zip(q.open.iter())
        .zip(q.high.iter())
        .zip(q.low.iter())
        .zip(q.close.iter())
        .map(|((((time, open), high), low), close)| Point{
            time: *time,
            open: *open,
            high: *high,
            low: *low,
            close: *close,
        })
        .collect())
    }
}

#[derive(Deserialize)]
struct TickData {
    timestamp: Vec<u32>,
    indicators: Indicator,
}

#[derive(Deserialize)]
struct Indicator {
    quote: Vec<DataPoints>,
}

#[derive(Deserialize)]
struct DataPoints {
    low: Vec<f32>,
    close: Vec<f32>,
    open: Vec<f32>,
    high: Vec<f32>,
}

#[derive(Serialize)]
struct Point {
    time: u32,
    open: f32,
    high: f32,
    low: f32,
    close: f32,
}

fn main() {
    let mut stocks = csv::Reader::from_path("ticker_list.csv").unwrap();

    let counter = Arc::new(RwLock::new(0));

    let client = Arc::new(RwLock::new(Client::new()));

    for stock in stocks.records() {
        let stock = stock.unwrap()[0].to_owned();
        let url = format!("{}{}{}", YAHOO1, stock, YAHOO2);

        let counter = counter.clone();
        let client = client.clone();

        while counter.read().unwrap().ge(&128) {
            //spin
            std::thread::sleep(Duration::from_millis(10));
        }

        *(counter.write().unwrap()) += 1;

        std::thread::spawn(move || {
            let r = client.read().unwrap().get(url).send().unwrap();
            match r.json::<ChartResp>() {
                Ok(r) => {
                if let Some(data) = r.to_tuples() {
                    let mut output = csv::Writer::from_path(format!("data/{}.csv", stock.replace("/", "_"))).unwrap();
                    for p in data {
                        output.serialize(p).unwrap();
                    }
                }
                else {
                    dbg!("bad :(");
                }  
            }
            Err(_e) => {
                //let r = client.read().unwrap().get(url).send().unwrap();
                //std::fs::write(format!("data/{}.json", stock.replace("/", "_")), format!("{}\n{}", e, r.text().unwrap())).unwrap();
            }
        }

            *(counter.write().unwrap()) -= 1;
        });
    }

    while counter.read().unwrap().gt(&0) {
        //spin
        std::thread::sleep(Duration::from_millis(1000));
    }
}
