use polars::prelude::*;

#[derive(Debug, Clone, Copy)]
pub struct TbLabel {
    pub outcome: TBOutcome,
    pub t_hit: usize,
    pub upper_level: i64,
    pub lower_level: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TBOutcome {
    TakeHit = 1, // bateu take primeiro
    StopHit = 2, // bateu stop primeiro
    Timeout = 3, // não bateu nenhum antes do horizonte
}

impl TBOutcome {
    #[inline]
    pub fn as_i8(&self) -> i8 {
        match self {
            TBOutcome::TakeHit => 1,
            TBOutcome::StopHit => -1,
            TBOutcome::Timeout => 0,
        }
    }

    #[inline]
    pub fn hit_code(self) -> i8 {
        match self {
            TBOutcome::TakeHit => 1,
            TBOutcome::StopHit => 2,
            TBOutcome::Timeout => 3,
        }
    }
}

#[inline]
fn first_passage(
    close: &[i64],
    start_idx: usize,
    t_end: usize,
    upper: i64,
    lower: i64,
) -> TBOutcome {
    // Política de tie-break: testamos upper antes de lower.
    // Em Renko empates perfeitos são raros; se quiser, pode inverter para testar robustez.
    let mut i = start_idx + 1;
    while i <= t_end {
        let p = close[i];
        if p >= upper {
            return TBOutcome::TakeHit;
        }
        if p <= lower {
            return TBOutcome::StopHit;
        }
        i += 1;
    }
    TBOutcome::Timeout
}

pub fn triple_barrier_labels(
    close_ticks: &[i64],
    brick_tick: i64,
    up_bricks: i64,
    down_bricks: i64,
    horizon_bricks: usize,
    conservative_tail: bool,
) -> Vec<Option<TbLabel>> {
    let n = close_ticks.len();
    let mut out = vec![None; n];

    if n == 0 || brick_tick <= 0 || up_bricks <= 0 || down_bricks <= 0 || horizon_bricks == 0 {
        return out;
    }

    for t in 0..n {
        // se opção conservadora estiver ativa e faltarem bricks suficientes, não rotula
        if conservative_tail && t + horizon_bricks >= n {
            out[t] = None;
            continue;
        }

        let p0 = close_ticks[t];
        let upper = p0 + up_bricks * brick_tick;
        let lower = p0 - down_bricks * brick_tick;
        let t_end = (t + horizon_bricks).min(n - 1);

        let outcome = first_passage(close_ticks, t, t_end, upper, lower);

        // tempo até o evento (se timeout, é t_end)
        let t_hit = match &outcome {
            TBOutcome::Timeout => t_end,
            _ => {
                // revarre uma curta faixa para localizar o índice exato do hit
                // (opcional: guardar durante a busca para evitar esta pequena segunda passada)
                let mut i = t + 1;
                while i <= t_end {
                    let p = close_ticks[i];
                    if (outcome == TBOutcome::TakeHit && p >= upper)
                        || (outcome == TBOutcome::StopHit && p <= lower)
                    {
                        break;
                    }
                    i += 1;
                }
                i
            }
        };

        out[t] = Some(TbLabel {
            outcome,
            t_hit,
            upper_level: upper,
            lower_level: lower,
        });
    }
    out
}

/// Anexa colunas do triple-barrier ao DataFrame:
/// - tb_label:  +1 / 0 / -1
/// - tb_hit_code: 1=upper, 2=lower, 3=timeout
/// - tb_t_hit: índice do primeiro evento (ou timeout)
/// - tb_time_to_hit: (t_hit - t)
/// - tb_upper, tb_lower: níveis em ticks
pub fn attach_tb_to_df(
    mut df: DataFrame,
    close_col: &str,
    brick_tick: i64,
    up_bricks: i64,
    down_bricks: i64,
    horizon_bricks: usize,
    conservative_tail: bool,
) -> Result<DataFrame, PolarsError> {
    let close = df.column(close_col)?.i64()?.into_no_null_iter().collect::<Vec<_>>();
    let labels = triple_barrier_labels(
        &close,
        brick_tick,
        up_bricks,
        down_bricks,
        horizon_bricks,
        conservative_tail,
    );

    let n = close.len();

    // colunas como Option para suportar "None" (últimos H, se conservative_tail=true)
    let mut y: Vec<Option<i8>> = Vec::with_capacity(n);
    let mut hit_code: Vec<Option<i8>> = Vec::with_capacity(n);
    let mut t_hit: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut time_to_hit: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut upper: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut lower: Vec<Option<i64>> = Vec::with_capacity(n);

    for (t, rec) in labels.into_iter().enumerate() {
        if let Some(tb) = rec {
            let label = tb.outcome.as_i8();      // +1 / 0 / -1
            let code = tb.outcome.hit_code();    // 1 / 2 / 3
            let thit = tb.t_hit as i64;
            let d = (tb.t_hit - t) as i64;

            y.push(Some(label));
            hit_code.push(Some(code));
            t_hit.push(Some(thit));
            time_to_hit.push(Some(d));
            upper.push(Some(tb.upper_level));
            lower.push(Some(tb.lower_level));
        } else {
            y.push(None);
            hit_code.push(None);
            t_hit.push(None);
            time_to_hit.push(None);
            upper.push(None);
            lower.push(None);
        }
    }

    df.with_column(Series::new("tb_label".into(), y))?
        .with_column(Series::new("tb_hit_code".into(), hit_code))?
        .with_column(Series::new("tb_t_hit".into(), t_hit))?
        .with_column(Series::new("tb_time_to_hit".into(), time_to_hit))?
        .with_column(Series::new("tb_upper".into(), upper))?
        .with_column(Series::new("tb_lower".into(), lower))?;

    Ok(df)
}
