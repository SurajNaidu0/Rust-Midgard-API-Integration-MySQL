use actix_web::{get, web, HttpResponse, Result};
use dotenv::dotenv;
use mysql::{self, prelude::*, Pool, Opts, Value};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Deserialize)]
pub struct DepthsQuery {
    pool: String,
    from: Option<i64>,
    to: Option<i64>,
    page: Option<i32>,
    interval: Option<i32>,
}

#[derive(Serialize)]
pub struct DepthInterval {
    assetDepth: String,
    assetPrice: String,
    assetPriceUSD: String,
    endTime: String,
    liquidityUnits: String,
    luvi: String,
    membersCount: String,
    runeDepth: String,
    startTime: String,
    synthSupply: String,
    synthUnits: String,
    units: String,
}

#[derive(Serialize)]
pub struct AggregatedStats {
    avgAssetDepth: String,
    avgAssetPrice: String,
    avgAssetPriceUSD: String,
    avgLiquidityUnits: String,
    avgLuvi: String,
    avgMembersCount: String,
    avgRuneDepth: String,
    avgSynthSupply: String,
    avgSynthUnits: String,
    avgUnits: String,
    totalIntervals: i32,
    timeRangeStart: String,
    timeRangeEnd: String,
}

#[derive(Serialize)]
pub struct DepthsResponse {
    intervals: Vec<DepthInterval>,
    aggregatedStats: AggregatedStats,  // This field name needs to match how we use it below
}

const POOL_MAPPINGS: &[(&str, &str)] = &[
    ("ETH.USDT-0XDAC17F958D2EE523A2206206994597C13D831EC7", "ETHUSDT0XDAC17F958D2EE523A2206206994594597C13D831EC7"),
    ("ETH.USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48", "ETHUSDC0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48"),
    ("BTC.BTC", "BTCBTC"),
    ("ETH.ETH", "ETHETH"),
];

pub fn establish_connection() -> Pool {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL not found in .env file");
    let opts = Opts::from_url(&database_url).expect("Invalid database URL");
    Pool::new(opts).unwrap()
}

fn value_to_string(value: Option<Value>) -> String {
    match value {
        Some(Value::Int(i)) => i.to_string(),
        Some(Value::UInt(i)) => i.to_string(),
        Some(Value::Float(f)) => format!("{:.8}", f),
        Some(Value::Double(d)) => format!("{:.8}", d),
        Some(v) => v.as_sql(false).to_string(),
        None => "0".to_string(),
    }
}

fn calculate_average(values: &[String]) -> String {
    if values.is_empty() {
        return "0".to_string();
    }

    let sum: f64 = values
        .iter()
        .map(|v| v.parse::<f64>().unwrap_or(0.0))
        .sum();

    format!("{:.8}", sum / values.len() as f64)
}

#[get("/depths")]
pub async fn get_depths(
    query: web::Query<DepthsQuery>,
    db_pool: web::Data<Pool>,
) -> Result<HttpResponse> {
    let table_name = POOL_MAPPINGS
        .iter()
        .find(|(key, _)| *key == query.pool)
        .map(|(_, value)| *value)
        .ok_or_else(|| actix_web::error::ErrorBadRequest("Invalid pool"))?;

    let mut sql = format!(
        "SELECT
            asset_depth, asset_price, asset_price_usd,
            UNIX_TIMESTAMP(end_time) as end_time,
            liquidity_units, luvi, members_count,
            rune_depths, UNIX_TIMESTAMP(start_time) as start_time,
            synth_supply, synth_units, units
        FROM {} WHERE 1=1",
        table_name
    );

    let mut params: Vec<Value> = Vec::new();

    if let Some(from) = query.from {
        sql.push_str(" AND start_time >= FROM_UNIXTIME(?)");
        params.push(from.into());
    }

    if let Some(to) = query.to {
        sql.push_str(" AND end_time <= FROM_UNIXTIME(?)");
        params.push(to.into());
    }

    let page = query.page.unwrap_or(1);
    let limit = query.interval.unwrap_or(100);
    sql.push_str(" ORDER BY start_time DESC LIMIT ? OFFSET ?");
    params.push(limit.into());
    params.push(((page - 1) * limit).into());

    let mut conn = db_pool.get_ref().get_conn()
        .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;

    let rows: Vec<mysql::Row> = conn.exec(&sql, params)
        .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;

    let mut asset_depths = Vec::new();
    let mut asset_prices = Vec::new();
    let mut asset_prices_usd = Vec::new();
    let mut liquidity_units = Vec::new();
    let mut luvis = Vec::new();
    let mut members_counts = Vec::new();
    let mut rune_depths = Vec::new();
    let mut synth_supplies = Vec::new();
    let mut synth_units = Vec::new();
    let mut units = Vec::new();
    let mut min_start_time = String::new();
    let mut max_end_time = String::new();

    let intervals: Vec<DepthInterval> = rows
        .into_iter()
        .map(|row| {
            let interval = DepthInterval {
                assetDepth: value_to_string(row.get("asset_depth")),
                assetPrice: value_to_string(row.get("asset_price")),
                assetPriceUSD: value_to_string(row.get("asset_price_usd")),
                endTime: value_to_string(row.get("end_time")),
                liquidityUnits: value_to_string(row.get("liquidity_units")),
                luvi: value_to_string(row.get("luvi")),
                membersCount: value_to_string(row.get("members_count")),
                runeDepth: value_to_string(row.get("rune_depths")),
                startTime: value_to_string(row.get("start_time")),
                synthSupply: value_to_string(row.get("synth_supply")),
                synthUnits: value_to_string(row.get("synth_units")),
                units: value_to_string(row.get("units")),
            };

            // Collect values for averaging
            asset_depths.push(interval.assetDepth.clone());
            asset_prices.push(interval.assetPrice.clone());
            asset_prices_usd.push(interval.assetPriceUSD.clone());
            liquidity_units.push(interval.liquidityUnits.clone());
            luvis.push(interval.luvi.clone());
            members_counts.push(interval.membersCount.clone());
            rune_depths.push(interval.runeDepth.clone());
            synth_supplies.push(interval.synthSupply.clone());
            synth_units.push(interval.synthUnits.clone());
            units.push(interval.units.clone());

            // Track time range
            if min_start_time.is_empty() || interval.startTime < min_start_time {
                min_start_time = interval.startTime.clone();
            }
            if max_end_time.is_empty() || interval.endTime > max_end_time {
                max_end_time = interval.endTime.clone();
            }

            interval
        })
        .collect();

    let aggregated_stats = AggregatedStats {
        avgAssetDepth: calculate_average(&asset_depths),
        avgAssetPrice: calculate_average(&asset_prices),
        avgAssetPriceUSD: calculate_average(&asset_prices_usd),
        avgLiquidityUnits: calculate_average(&liquidity_units),
        avgLuvi: calculate_average(&luvis),
        avgMembersCount: calculate_average(&members_counts),
        avgRuneDepth: calculate_average(&rune_depths),
        avgSynthSupply: calculate_average(&synth_supplies),
        avgSynthUnits: calculate_average(&synth_units),
        avgUnits: calculate_average(&units),
        totalIntervals: intervals.len() as i32,
        timeRangeStart: min_start_time,
        timeRangeEnd: max_end_time,
    };

    Ok(HttpResponse::Ok().json(DepthsResponse {
        intervals,
        aggregatedStats: aggregated_stats  // Make sure this matches the struct field name
    }))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    use actix_web::{App, HttpServer};

    let pool = establish_connection();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .service(get_depths)
    })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}