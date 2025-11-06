pub mod features;

const PRICE_SCALE: i64 = 100;
#[derive(Debug, Clone)]
pub enum Direction {
    UP,
    DOWN,
}

#[derive(Debug, Clone)]
pub struct RenkoBrick {
    open: i64,
    close: i64,
}
impl RenkoBrick {
    pub fn direction(&self) -> Direction {
        if self.close > self.open {
            Direction::UP
        } else {
            Direction::DOWN
        }
    }
}

#[inline]
fn to_ticks(p: f64) -> i64 {
    (p * PRICE_SCALE as f64).round() as i64
}

#[inline]
fn from_ticks(t: i64) -> f64 {
    (t as f64) / PRICE_SCALE as f64
}


pub fn generate_renko_bricks_ticks(prices: &[f64], brick_size: f64) -> Vec<RenkoBrick> {
    if prices.is_empty() {
        return vec![];
    }

    let brick = to_ticks(brick_size);

    // ancora o primeiro preço ao múltiplo de brick mais próximo (reduz drift)
    let mut last = {
        let p0 = to_ticks(prices[0]);
        (p0 as f64 / brick as f64).round() as i64 * brick
    };

    let mut out = Vec::new();

    for &p in prices.iter().skip(1) {
        let curr = to_ticks(p);
        let diff = curr - last;
        let n = (diff.abs() / brick) as i32;
        if n > 0 {
            let dir = diff.signum(); // -1, 0, 1
            for _ in 0..n {
                let open = last;
                let close = open + dir * brick;
                out.push(RenkoBrick { open, close });
                last = close;
            }
        }
    }
    out
}

pub fn plot_renko_chart_ticks(
    bricks: &[RenkoBrick],
    brick_size: f64,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use plotters::prelude::*;
    use plotters_bitmap::BitMapBackend;

    if bricks.is_empty() {
        println!("No data to plot");
        return Ok(());
    }

    let root = BitMapBackend::new(path, (1280, 720)).into_drawing_area();
    root.fill(&WHITE)?;

    // min/max em ticks
    let (mut y_min_t, mut y_max_t) = bricks.iter().fold((i64::MAX, i64::MIN), |acc, b| {
        (
            acc.0.min(b.open).min(b.close),
            acc.1.max(b.open).max(b.close),
        )
    });

    // margem de 2 tijolos
    let brick_t = to_ticks(brick_size);
    y_min_t -= 2 * brick_t;
    y_max_t += 2 * brick_t;

    // converte para f64 para o eixo Y
    let y_min = from_ticks(y_min_t);
    let y_max = from_ticks(y_max_t);
    let x_max = bricks.len() as i32;

    let mut chart = ChartBuilder::on(&root)
        .caption("Gráfico Renko", ("sans-serif", 30).into_font())
        .margin(10)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(0..x_max, y_min..y_max)?;

    chart
        .configure_mesh()
        .x_desc("Índice do Brick")
        .y_desc("Preço")
        .y_label_formatter(&|y| format!("{:.2}", y))
        .draw()?;

    chart.draw_series(bricks.iter().enumerate().map(|(idx, b)| {
        let color = match b.direction() {
            Direction::UP => { GREEN.filled() }
            Direction::DOWN => { RED.filled() }
        };

        // retângulo em coordenadas (x0..x1, y0..y1)
        let x0 = idx as i32;
        // overlap mínimo para evitar linha branca de rasterização
        let x1 = (idx + 1) as i32;

        let (min_y, max_y) = if b.open < b.close {
            (from_ticks(b.open), from_ticks(b.close))
        } else {
            (from_ticks(b.close), from_ticks(b.open))
        };

        Rectangle::new([(x0, min_y), (x1, max_y)], color)
    }))?;

    root.present()?;
    println!("Gráfico Renko salvo em '{}'", path);
    Ok(())
}
