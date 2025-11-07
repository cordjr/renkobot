mod data;
mod renko;
mod ml;
mod utils;

use clap::{Parser, Subcommand};

use crate::renko::{generate_renko_bricks_ticks, plot_renko_chart_ticks};
use std::error::Error;
use polars::prelude::IntoLazy;
use crate::ml::labeling::attach_tb_to_df;
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
    let lf = df.clone().lazy();
    let feats = extract_features(lf).collect()?;
    println!("{}", feats.head(Some(10)));
    println!("plotting renko chart...");

    // SCALE=100 no teu módulo; ex.: brick_size=50.0 => brick_tick=5000
    let brick_size = 50.0;
    let brick_tick = (100.0 * brick_size) as i64;

    // hiperparâmetros iniciais
    let up_bricks = 3;
    let down_bricks = 2;
    let horizon = 12;

    // conservador: não rotula últimos H
    let df_labeled = attach_tb_to_df(
        df,
        "close_ticks",
        brick_tick,
        up_bricks,
        down_bricks,
        horizon,
        true, // conservative_tail
    )?;

    println!("{}", df_labeled.head(Some(10)));
    plot_renko_chart_ticks(&bricks, brick_size, "renko_chart.png")?;

    println!("chart saved");

    Ok(())

    // println!("{:?}", df.head(None));
}

