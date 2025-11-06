use super::{Direction, RenkoBrick};
use anyhow::Result;
use polars::df;
use polars::prelude::RollingOptionsFixedWindow;
use polars::prelude::*;
use polars::series::ops::NullBehavior;

pub fn bricks_to_df(bricks: &[RenkoBrick]) -> Result<DataFrame> {
    let open: Vec<i64> = bricks.iter().map(|b| b.open).collect();
    let close: Vec<i64> = bricks.iter().map(|b| b.close).collect();
    let dir: Vec<i64> = bricks
        .iter()
        .map(|b| match b.direction() {
            Direction::UP => 1,
            Direction::DOWN => -1,
        })
        .collect();
    let df = df![
        "open_ticks" => open,
        "close_ticks" => close,
        "dir" => dir
    ]?;
    Ok(df)
}

pub fn add_run_length(df: LazyFrame) -> LazyFrame {
    // 1) Marca o início de um novo run (true/false)
    //    - Primeiro brick (shift = null) deve ser "novo run" => fill_null(true)
    let df = df.with_column(
        col("dir")
            .neq(col("dir").shift(lit(1)))
            .fill_null(lit(true))
            .alias("is_new_run"), // bool
    );

    // 2) Constrói um "run_id" por cumsum dos inícios (precisa converter bool->int)
    let df = df.with_column(
        col("is_new_run")
            .cast(DataType::Int32)
            .cum_sum(false)
            .alias("run_id"), // 1,1,1,2,2,3...
    );

    // 3) Agora geramos o contador acumulado DENTRO de cada run:
    //    truque: some 1's cumulativamente over(run_id)
    let df = df.with_column(lit(1).alias("one")).with_column(
        col("one")
            .cum_sum(false) // contador global
            .over([col("run_id")]) // reinicia por run_id
            .alias("run_len"), // 1,2,3,1,2,1...
    );


    // 4) Opcional: limpe colunas temporárias
    let columns = [cols(vec!["is_new_run", "one"])];
    df.drop(columns)
}
pub fn add_momentum(df: LazyFrame, n: i64) -> LazyFrame {
    df.with_column(
        ((col("close_ticks") - col("close_ticks").shift(lit(n)))
            / col("close_ticks").shift(lit(n)))
        .alias(&format!("mom_{}", n)),
    )
}

pub fn add_slope(df: LazyFrame, window: usize) -> LazyFrame {
    let rolling_window = rolling_window(window.clone());
    df.with_columns([
        col("close_ticks")
            .rolling_mean(rolling_window.clone())
            .alias(&format!("ma_{}", window)),
        col("close_ticks")
            .diff(1i64,  NullBehavior::Ignore)
            .rolling_mean(rolling_window.clone())
            .alias(&format!("slope_{}", window))]
    )
}

fn rolling_window(window: usize) -> RollingOptionsFixedWindow {
    RollingOptionsFixedWindow {
        window_size: window,
        min_periods: 1,
        center: false,
        ..Default::default()
    }
}
pub fn add_entropy(df: LazyFrame, window: usize) -> LazyFrame {

    let p_up = col("dir")
        .rolling_mean(rolling_window(window.clone()));

    df.with_column(
        (-((p_up.clone() * p_up.clone().log(2.0))
            + ((lit(1.0) - p_up.clone()) * (lit(1.0) - p_up).log(2.0))))
            .alias(&format!("entropy_{}", window)),
    )
}

pub fn extract_features(df: LazyFrame) -> LazyFrame {
    let df = add_run_length(df);
    let df = add_momentum(df, 3);
    let df = add_slope(df, 10);
    let df = add_entropy(df, 20);
    df
}