use polars::datatypes::TimeUnit;
use polars::prelude::{CsvReader, IntoLazy, SerReader, StrptimeOptions, col, lit};
use std::error::Error;
use std::fs::File;

pub fn load_csv(file: String) -> Result<Vec<f64>, Box<dyn Error>> {
    let csv_file = File::open(file)?;

    let mut df = CsvReader::new(csv_file).finish()?;
    df = df
        .clone()
        .lazy()
        .with_column(
            col("CloseTime")
                .str()
                .to_datetime(
                    Some(TimeUnit::Microseconds),
                    None,
                    StrptimeOptions::default(),
                    lit("raise"),
                )
                .alias("CloseTime"),
        )
        .collect()?;

    let binding = df
        .clone()
        .lazy()
        .select([col("CloseTime").max()])
        .collect()?;

    let last_date = binding
        .column("CloseTime")?
        .datetime()?
        .as_datetime_iter()
        .filter_map(|x| x)
        .last()
        .ok_or_else(|| anyhow::anyhow!("No data found"))?;

    let days_ago = last_date - chrono::Duration::days(7);
    let days_ago_timestamp = days_ago.and_utc().timestamp_micros();
    let df_result = df
        .clone()
        .lazy()
        .filter(
            col("CloseTime")
                .gt_eq(days_ago_timestamp)
                .alias("CloseTime"),
        )
        .collect();
    if let Err(err) = df_result {
        println!("Error: {}", err);
        return Err(err.to_string().into());
    }
    df = df_result.unwrap();

    let close_series = df.column("Close")?.f64()?;
    Ok(close_series.into_no_null_iter().collect::<Vec<f64>>())
}
