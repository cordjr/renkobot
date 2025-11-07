use anyhow::{anyhow, Result};
use linfa::prelude::*;
use ndarray::{Array1, Array2};
use polars::prelude::*;


#[derive(Debug, Clone, Default)]
pub struct Metrics {
    pub accuracy: f32,
    pub precision: f32,
    pub recall: f32,
    pub f1: f32,
    pub mcc: f32,
    pub tp: u32,
    pub tn: u32,
    pub fp: u32,
    pub fn_: u32,
}

impl Metrics {
    pub fn add(&mut self, other: &Metrics) {
        self.accuracy += other.accuracy;
        self.precision += other.precision;
        self.recall += other.recall;
        self.f1 += other.f1;
        self.mcc += other.mcc;
        self.tp += other.tp;
        self.tn += other.tn;
        self.fp += other.fp;
        self.fn_ += other.fn_;
    }
    pub fn div_mut(&mut self, k: f32) {
        self.accuracy /= k;
        self.precision /= k;
        self.recall /= k;
        self.f1 /= k;
        self.mcc /= k;
        // TP/TN/FP/FN não são médias — mantenha somados ou divida se quiser taxa média
    }
}

pub fn xy_from_df<S>(
    df: &DataFrame,
    feature_cols: &[&str],
    label_col: &str,
) -> Result<(Array2<f64>, Array1<i32>)> {
    // Seleciona features e label, filtra linhas com null
    let mut cols: Vec<&str> = feature_cols.to_vec();
    cols.push(label_col);
    let df = df.select(&cols)?;
    let df  = df.drop_nulls::<&str>(None)?;

    let y_s = df.column(label_col)?.i8()?;
    let y: Array1<i32> = y_s.into_no_null_iter().map(|value| value as i32).collect();


    let mut x_mat: Vec<f64> = Vec::with_capacity(feature_cols.len() * df.height());
    for column_name in feature_cols.iter() {
        let s = df.column(column_name)?;
        if let Ok(c) = s.f64() {
            for v in c.into_no_null_iter() { x_mat.push(v); }
        } else if let Ok(c) = s.i64() {
            for v in c.into_no_null_iter() { x_mat.push(v as f64); }
        } else if let Ok(c) = s.i32() {
            for v in c.into_no_null_iter() { x_mat.push(v as f64); }
        } else if let Ok(c) = s.i8() {
            for v in c.into_no_null_iter() { x_mat.push(v as f64); }
        } else {
            return Err(anyhow!("Unsupported feature dtype for column {}", column_name));
        }
    }

    // Organiza em Array2 na forma (n_samples, n_features)
    let n = df.height();
    let m = feature_cols.len();
    // x_mat está colado por colunas; precisamos transpô-lo para (row-major)
    // Estratégia simples: construir por linhas
    let mut x = Array2::<f64>::zeros((n, m));
    for (j, col_name) in feature_cols.iter().enumerate() {
        let s = df.column(col_name)?;
        if let Ok(c) = s.f64() {
            for (i, v) in c.into_no_null_iter().enumerate() { x[[i, j]] = v; }
        } else if let Ok(c) = s.i64() {
            for (i, v) in c.into_no_null_iter().enumerate() { x[[i, j]] = v as f64; }
        } else if let Ok(c) = s.i32() {
            for (i, v) in c.into_no_null_iter().enumerate() { x[[i, j]] = v as f64; }
        } else if let Ok(c) = s.i8() {
            for (i, v) in c.into_no_null_iter().enumerate() { x[[i, j]] = v as f64; }
        }
    }
    Ok((x, y))
}

pub struct TemporalSplit {
    pub train_start: usize,
    pub train_end: usize,   // exclusivo
    pub test_end: usize,    // exclusivo
}

pub fn build_walkforward_splits(
    n: usize,
    train_ratio: f32,  // ex.: 0.7
    test_ratio: f32,   // ex.: 0.2
    step_ratio: f32,   // ex.: 0.1 (quanto andamos a janela)
) -> Vec<TemporalSplit> {
    let mut splits = vec![];

    let train_len = (n as f32 * train_ratio).max(1.0) as usize;
    let test_len = (n as f32 * test_ratio).max(1.0) as usize;
    let step = (n as f32 * step_ratio).max(1.0) as usize;

    let mut start = 0usize;
    loop {
        let train_start = start;
        let train_end = start + train_len;
        let test_end = start + test_len;
        if test_end > n {
            break;
        }
        splits.push(TemporalSplit { train_start, train_end, test_end });
        if train_end + step >= n { break; }
        start += step;
    }
    splits
}
pub fn filter_binary_classes(x: &Array2<f64>, y: &Array1<i32>) -> (Array2<f64>, Array1<i32>) {
    let idxs: Vec<usize> = y
        .indexed_iter()
        .filter_map(|(i, &v)| if v == 1 || v == -1 { Some(i) } else { None })
        .collect();
    let n = idxs.len();
    let m = x.ncols();
    let mut x2 = Array2::<f64>::zeros((n, m));
    let mut y2 = Array1::<i32>::zeros(n);
    for (k, &i) in idxs.iter().enumerate() {
        y2[k] = y[i];
        for j in 0..m {x2[[k, j]] = x[[i, j]]}
    }
    (x2, y2)
}

pub fn logistic_regression_example() {
    println!("Rodando regressão logística (em breve)");
}