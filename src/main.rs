use std::fs::File;
use std::path::Path;
use clap::{Error, Parser, Subcommand};
use log::error;
use polars::prelude::{CsvReadOptions, CsvReader, SerReader};

#[derive(Parser, Debug)]
#[clap(author = "BTC Strategy Developer", version, about = "Bitcoin Trading Strategy")]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}
#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a backtest on historical data
    Plot {}
}

fn main() {

    // Parse command line arguments
    let args = Args::try_parse();
    match args {
        Ok(arg) => {
            match arg.command {
                Commands::Plot {} => plot(),
            }
        }
        Err(err) => {
            println!("Error: {}", err);

        }
    }

}

fn plot() {
    load_data();

}


fn load_data() {
    let file_result = File::open("eth_5m_futures.csv");
    if  let Err(e) =  file_result {
        println!("Error opening file: {}", e);
        return;
    }
    let csv_file = file_result.unwrap();

    let csv_result = CsvReader::new(csv_file).finish();
    if let Err(e) = csv_result {
        println!( "Error reading CSV: {}", e);
        return;
    }
    let df = csv_result.unwrap();
    println!("{:?}",df.head(None) );
}