mod data;
mod renko;
mod ml;
mod utils;

use clap::{Parser, Subcommand};

use crate::renko::{generate_renko_bricks_ticks, plot_renko_chart_ticks};
use std::error::Error;
use polars::prelude::IntoLazy;
use crate::renko::features::{bricks_to_df, extract_features};

#[derive(Parser, Debug)]
#[clap(
    author = "BTC Strategy Developer",
    version,
    about = "Bitcoin Trading Strategy"
)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}
#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a backtest on historical data
    Plot {
        #[arg(short, long, default_value = "eth_5m_futures.csv")]
        file: String,
    },
}




fn main() {
    // Parse command line arguments
    let args = Args::try_parse();
    match args {
        Ok(arg) => match arg.command {
            Commands::Plot{ file}  => plot(file),
        },
        Err(err) => {
            println!("Error: {}", err);
        }
    }
}

fn plot(file: String) {
    match load_data(file) {
        Ok(_) => {
            print!("Process finished successfully.")
        }
        Err(err) => {
            print!("Process finished with errors. {:?}", err)
        }
    }
}

fn load_data(file: String) -> Result<(), Box<dyn Error>> {


    let brick_size = 10.0;

    println!("generating renko bricks (ticks)...");
    let prices  = data::load_csv(file)?;
    let bricks = generate_renko_bricks_ticks(&prices, brick_size);
    log::info!("🔍 Extraindo features...");
    let df = bricks_to_df(&bricks)?;
    let lf = df.lazy();
    let feats = extract_features(lf).collect()?;
    println!("{}", feats.head(Some(10)));
    println!("plotting renko chart...");
    plot_renko_chart_ticks(&bricks, brick_size, "renko_chart.png")?;

    println!("chart saved");

    Ok(())

    // println!("{:?}", df.head(None));
}

