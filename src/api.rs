use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use chrono::{DateTime, Utc, TimeZone};
use mysql::{Pool, Row};
use mysql::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use crate::MidgardData::DATA;

// Query parameters struct for /swaphistory
#[derive(Debug, Deserialize)]
struct SwapHistoryParams {
    pool: String,
    #[serde(default)]
    from: Option<i64>,
    #[serde(default)]
    to: Option<i64>,
    #[serde(default)]
    interval: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default)]
    offset: Option<u64>,
}

// Query parameters struct for /earninghistory
#[derive(Debug, Deserialize)]
struct EarningHistoryParams {
    pool: Option<String>, // Optional to support network-wide earnings
    #[serde(default)]
    from: Option<i64>,
    #[serde(default)]
    to: Option<i64>,
    #[serde(default)]
    interval: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default)]
    offset: Option<u64>,
}

// Query parameters struct for /depthandprice
#[derive(Debug, Deserialize)]
struct DepthAndPriceParams {
    pool: String, // Required, like swaphistory
    #[serde(default)]
    from: Option<i64>,
    #[serde(default)]
    to: Option<i64>,
    #[serde(default)]
    interval: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default)]
    offset: Option<u64>,
}

// Helper function to get string value from a database row
fn get_string_value(row: &Row, field: &str) -> String {
    if let Some(Ok(value)) = row.get_opt::<String, _>(field) {
        return value;
    }
    if let Some(Ok(value)) = row.get_opt::<i64, _>(field) {
        return value.to_string();
    }
    if let Some(Ok(value)) = row.get_opt::<f64, _>(field) {
        return value.to_string();
    }
    "".to_string()
}

// Map external pool name to database table name
fn map_pool_to_table(pool_name: &str) -> Option<&'static str> {
    DATA.iter()
        .find(|(key, _)| *key == pool_name)
        .map(|(_, value)| *value)
}

// Handler for /swaphistory endpoint
async fn handle_swap_history(
    Query(params): Query<SwapHistoryParams>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    println!("Received request for pool: {}", params.pool);

    let table_name = match map_pool_to_table(&params.pool) {
        Some(name) => {
            println!("Mapped to table: {}", name);
            name
        },
        None => {
            println!("Failed to map pool name: {}", params.pool);
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("Invalid pool name: {}", params.pool)
                })),
            );
        }
    };

    let from = params.from.unwrap_or_else(|| {
        let start_of_2020 = Utc.ymd(2020, 1, 1).and_hms(0, 0, 0).timestamp();
        println!("No 'from' parameter specified, defaulting to 2020-01-01");
        start_of_2020
    });

    let to = params.to.unwrap_or_else(|| {
        let end_of_2030 = Utc.ymd(2030, 12, 31).and_hms(23, 59, 59).timestamp();
        println!("No 'to' parameter specified, defaulting to 2030-12-31");
        end_of_2030
    });
    let interval = params.interval.unwrap_or_else(|| "hour".to_string());
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    let from_date = Utc.timestamp(from, 0).format("%Y-%m-%d %H:%M:%S").to_string();
    let to_date = Utc.timestamp(to, 0).format("%Y-%m-%d %H:%M:%S").to_string();

    println!("Query date range: {} to {}", from_date, to_date);

    let pool = &state.pool;
    let mut conn = match pool.get_conn() {
        Ok(conn) => conn,
        Err(e) => {
            println!("Database connection error: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Database connection error: {}", e)
                })),
            );
        }
    };

    let meta_query = format!(
        "SELECT
         SUM(to_asset_count) as to_asset_count,
         SUM(to_rune_count) as to_rune_count,
         SUM(to_trade_count) as to_trade_count,
         SUM(from_trade_count) as from_trade_count,
         SUM(to_secured_count) as to_secured_count,
         SUM(from_secured_count) as from_secured_count,
         SUM(synth_mint_count) as synth_mint_count,
         SUM(synth_redeem_count) as synth_redeem_count,
         SUM(total_count) as total_count,
         SUM(to_asset_volume) as to_asset_volume,
         SUM(to_rune_volume) as to_rune_volume,
         SUM(to_trade_volume) as to_trade_volume,
         SUM(from_trade_volume) as from_trade_volume,
         SUM(to_secured_volume) as to_secured_volume,
         SUM(from_secured_volume) as from_secured_volume,
         SUM(synth_mint_volume) as synth_mint_volume,
         SUM(synth_redeem_volume) as synth_redeem_volume,
         SUM(total_volume) as total_volume,
         SUM(to_asset_volume_usd) as to_asset_volume_usd,
         SUM(to_rune_volume_usd) as to_rune_volume_usd,
         SUM(to_trade_volume_usd) as to_trade_volume_usd,
         SUM(from_trade_volume_usd) as from_trade_volume_usd,
         SUM(to_secured_volume_usd) as to_secured_volume_usd,
         SUM(from_secured_volume_usd) as from_secured_volume_usd,
         SUM(synth_mint_volume_usd) as synth_mint_volume_usd,
         SUM(synth_redeem_volume_usd) as synth_redeem_volume_usd,
         SUM(total_volume_usd) as total_volume_usd,
         SUM(to_asset_fees) as to_asset_fees,
         SUM(to_rune_fees) as to_rune_fees,
         SUM(to_trade_fees) as to_trade_fees,
         SUM(from_trade_fees) as from_trade_fees,
         SUM(to_secured_fees) as to_secured_fees,
         SUM(from_secured_fees) as from_secured_fees,
         SUM(synth_mint_fees) as synth_mint_fees,
         SUM(synth_redeem_fees) as synth_redeem_fees,
         SUM(total_fees) as total_fees,
         AVG(to_asset_average_slip) as to_asset_average_slip,
         AVG(to_rune_average_slip) as to_rune_average_slip,
         AVG(to_trade_average_slip) as to_trade_average_slip,
         AVG(from_trade_average_slip) as from_trade_average_slip,
         AVG(to_secured_average_slip) as to_secured_average_slip,
         AVG(from_secured_average_slip) as from_secured_average_slip,
         AVG(synth_mint_average_slip) as synth_mint_average_slip,
         AVG(synth_redeem_average_slip) as synth_redeem_average_slip,
         AVG(average_slip) as average_slip,
         AVG(rune_price_usd) as rune_price_usd
        FROM {}
        WHERE start_time >= ? AND end_time <= ?",
        table_name
    );

    println!("Executing meta query with params: {} and {}", from_date, to_date);

    let check_query = format!("SELECT COUNT(*) as count FROM {}", table_name);
    let count_result: Result<Option<Row>, mysql::Error> = conn.query_first(&check_query);

    match count_result {
        Ok(Some(row)) => {
            let count: i64 = row.get("count").unwrap_or(0);
            println!("Table {} has {} rows", table_name, count);

            let date_check = format!("SELECT MIN(start_time) as min_time, MAX(end_time) as max_time FROM {}", table_name);
            if let Ok(Some(row)) = conn.query_first::<Row, _>(&date_check) {
                let min_time: Option<String> = row.get("min_time");
                let max_time: Option<String> = row.get("max_time");
                println!("Data date range: {:?} to {:?}", min_time, max_time);
            }
        },
        Ok(None) => println!("No count returned for table {}", table_name),
        Err(e) => println!("Error checking table {}: {}", table_name, e),
    }

    let meta_row_result: Result<Option<Row>, mysql::Error> = conn.exec_first(meta_query, (from_date.clone(), to_date.clone()));

    let meta_row = match meta_row_result {
        Ok(Some(row)) => row,
        Ok(None) => {
            println!("No data found for the specified criteria");
            return (
                StatusCode::OK,
                Json(json!({
                    "meta": {},
                    "intervals": []
                })),
            );
        },
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Database query error: {}", e)
                })),
            );
        }
    };

    let fields = [
        "startTime", "endTime", "toAssetCount", "toRuneCount", "toTradeCount",
        "fromTradeCount", "toSecuredCount", "fromSecuredCount", "synthMintCount",
        "synthRedeemCount", "totalCount", "toAssetVolume", "toRuneVolume",
        "toTradeVolume", "fromTradeVolume", "toSecuredVolume", "fromSecuredVolume",
        "synthMintVolume", "synthRedeemVolume", "totalVolume", "toAssetVolumeUSD",
        "toRuneVolumeUSD", "toTradeVolumeUSD", "fromTradeVolumeUSD", "toSecuredVolumeUSD",
        "fromSecuredVolumeUSD", "synthMintVolumeUSD", "synthRedeemVolumeUSD",
        "totalVolumeUSD", "toAssetFees", "toRuneFees", "toTradeFees", "fromTradeFees",
        "toSecuredFees", "fromSecuredFees", "synthMintFees", "synthRedeemFees",
        "totalFees", "toAssetAverageSlip", "toRuneAverageSlip", "toTradeAverageSlip",
        "fromTradeAverageSlip", "toSecuredAverageSlip", "fromSecuredAverageSlip",
        "synthMintAverageSlip", "synthRedeemAverageSlip", "averageSlip", "runePriceUSD"
    ];

    let db_fields = [
        "start_time", "end_time", "to_asset_count", "to_rune_count", "to_trade_count",
        "from_trade_count", "to_secured_count", "from_secured_count", "synth_mint_count",
        "synth_redeem_count", "total_count", "to_asset_volume", "to_rune_volume",
        "to_trade_volume", "from_trade_volume", "to_secured_volume", "from_secured_volume",
        "synth_mint_volume", "synth_redeem_volume", "total_volume", "to_asset_volume_usd",
        "to_rune_volume_usd", "to_trade_volume_usd", "from_trade_volume_usd", "to_secured_volume_usd",
        "from_secured_volume_usd", "synth_mint_volume_usd", "synth_redeem_volume_usd",
        "total_volume_usd", "to_asset_fees", "to_rune_fees", "to_trade_fees", "from_trade_fees",
        "to_secured_fees", "from_secured_fees", "synth_mint_fees", "synth_redeem_fees",
        "total_fees", "to_asset_average_slip", "to_rune_average_slip", "to_trade_average_slip",
        "from_trade_average_slip", "to_secured_average_slip", "from_secured_average_slip",
        "synth_mint_average_slip", "synth_redeem_average_slip", "average_slip", "rune_price_usd"
    ];

    let mut meta_map = serde_json::Map::new();
    meta_map.insert("startTime".to_string(), Value::String(from_date.clone()));
    meta_map.insert("endTime".to_string(), Value::String(to_date.clone()));
    for (json_field, db_field) in fields.iter().zip(db_fields.iter()).skip(2) { // Skip startTime and endTime
        let value = get_string_value(&meta_row, db_field);
        meta_map.insert(json_field.to_string(), Value::String(value));
    }
    let meta = Value::Object(meta_map);

    let interval_format = match interval.as_str() {
        "day" => "%Y-%m-%d 00:00:00",
        "week" => "%Y-%v-1 00:00:00",
        "month" => "%Y-%m-01 00:00:00",
        "year" => "%Y-01-01 00:00:00",
        _ => "%Y-%m-%d %H:00:00",
    };

    let interval_query = format!(
        "SELECT
         DATE_FORMAT(start_time, '{}') as interval_start,
         MIN(start_time) as start_time,
         MAX(end_time) as end_time,
         SUM(to_asset_count) as to_asset_count,
         SUM(to_rune_count) as to_rune_count,
         SUM(to_trade_count) as to_trade_count,
         SUM(from_trade_count) as from_trade_count,
         SUM(to_secured_count) as to_secured_count,
         SUM(from_secured_count) as from_secured_count,
         SUM(synth_mint_count) as synth_mint_count,
         SUM(synth_redeem_count) as synth_redeem_count,
         SUM(total_count) as total_count,
         SUM(total_volume) as total_volume,
         SUM(total_volume_usd) as total_volume_usd,
         SUM(total_fees) as total_fees,
         AVG(average_slip) as average_slip
        FROM {}
        WHERE start_time >= ? AND end_time <= ?
        GROUP BY interval_start
        ORDER BY interval_start
        LIMIT ? OFFSET ?",
        interval_format, table_name
    );

    println!("Executing interval query");

    let interval_rows_result: Result<Vec<Row>, mysql::Error> = conn.exec(
        interval_query,
        (from_date, to_date, limit, offset)
    );

    let interval_rows = match interval_rows_result {
        Ok(rows) => rows,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Database query error: {}", e)
                })),
            );
        }
    };

    let mut intervals = Vec::new();
    for row in interval_rows {
        let mut interval_map = serde_json::Map::new();
        let interval_fields = [
            "startTime", "endTime", "toAssetCount", "toRuneCount", "toTradeCount",
            "fromTradeCount", "toSecuredCount", "fromSecuredCount", "synthMintCount",
            "synthRedeemCount", "totalCount", "totalVolume", "totalVolumeUSD",
            "totalFees", "averageSlip"
        ];
        let interval_db_fields = [
            "start_time", "end_time", "to_asset_count", "to_rune_count", "to_trade_count",
            "from_trade_count", "to_secured_count", "from_secured_count", "synth_mint_count",
            "synth_redeem_count", "total_count", "total_volume", "total_volume_usd",
            "total_fees", "average_slip"
        ];
        for (json_field, db_field) in interval_fields.iter().zip(interval_db_fields.iter()) {
            interval_map.insert(
                json_field.to_string(),
                Value::String(get_string_value(&row, db_field))
            );
        }
        intervals.push(Value::Object(interval_map));
    }

    println!("Returning response with {} intervals", intervals.len());

    (
        StatusCode::OK,
        Json(json!({
            "meta": meta,
            "intervals": intervals
        })),
    )
}

// Handler for /earninghistory endpoint
async fn handle_earning_history(
    Query(params): Query<EarningHistoryParams>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    println!("Received request for earning history");

    let (table_name, is_pool_specific) = match params.pool {
        Some(pool_name) => {
            match map_pool_to_table(&pool_name) {
                Some(name) => {
                    println!("Mapped to pool-specific table: {}", name);
                    (name, true)
                },
                None => {
                    println!("Invalid pool name: {}", pool_name);
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": format!("Invalid pool name: {}", pool_name)
                        })),
                    );
                }
            }
        },
        None => {
            println!("No pool specified, using RUNE_MEMBER for overall earnings");
            ("RUNE_MEMBER", false)
        }
    };

    let from = params.from.unwrap_or_else(|| {
        let start_of_2020 = Utc.ymd(2020, 1, 1).and_hms(0, 0, 0).timestamp();
        println!("No 'from' parameter specified, defaulting to 2020-01-01");
        start_of_2020
    });

    let to = params.to.unwrap_or_else(|| {
        let end_of_2030 = Utc.ymd(2030, 12, 31).and_hms(23, 59, 59).timestamp();
        println!("No 'to' parameter specified, defaulting to 2030-12-31");
        end_of_2030
    });
    let interval = params.interval.unwrap_or_else(|| "hour".to_string());
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    let from_date = Utc.timestamp(from, 0).format("%Y-%m-%d %H:%M:%S").to_string();
    let to_date = Utc.timestamp(to, 0).format("%Y-%m-%d %H:%M:%S").to_string();

    println!("Query date range: {} to {}", from_date, to_date);

    let pool = &state.pool;
    let mut conn = match pool.get_conn() {
        Ok(conn) => conn,
        Err(e) => {
            println!("Database connection error: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Database connection error: {}", e)
                })),
            );
        }
    };

    let (meta_query, fields, db_fields) = if is_pool_specific {
        (
            format!(
                "SELECT
                 SUM(asset_liquidity_fees) as asset_liquidity_fees,
                 SUM(rune_liquidity_fees) as rune_liquidity_fees,
                 SUM(total_liquidity_fees_rune) as total_liquidity_fees_rune,
                 SUM(earnings) as earnings,
                 SUM(saver_earning) as saver_earning,
                 SUM(rewards) as rewards,
                 AVG(rune_price_usd) as rune_price_usd
                FROM {}
                WHERE start_time >= ? AND end_time <= ?",
                table_name
            ),
            [
                "startTime", "endTime", "liquidityFees", "blockRewards", "earnings",
                "bondingEarnings", "liquidityEarnings", "avgNodeCount", "runePriceUSD"
            ],
            [
                "start_time", "end_time", "total_liquidity_fees_rune", "rewards", "earnings",
                "saver_earning", "asset_liquidity_fees", "rune_liquidity_fees", "rune_price_usd"
            ]
        )
    } else {
        (
            format!(
                "SELECT
                 SUM(liquidity_fees) as liquidity_fees,
                 SUM(block_rewards) as block_rewards,
                 SUM(earnings) as earnings,
                 SUM(bonding_earnings) as bonding_earnings,
                 SUM(liquidity_earning) as liquidity_earning,
                 AVG(avg_node_count) as avg_node_count
                FROM {}
                WHERE start_time >= ? AND end_time <= ?",
                table_name
            ),
            [
                "startTime", "endTime", "liquidityFees", "blockRewards", "earnings",
                "bondingEarnings", "liquidityEarnings", "avgNodeCount", "runePriceUSD"
            ],
            [
                "start_time", "end_time", "liquidity_fees", "block_rewards", "earnings",
                "bonding_earnings", "liquidity_earning", "avg_node_count", "0"
            ]
        )
    };

    println!("Executing meta query with params: {} and {}", from_date, to_date);

    let meta_row_result: Result<Option<Row>, mysql::Error> = conn.exec_first(&meta_query, (from_date.clone(), to_date.clone()));

    let meta_row = match meta_row_result {
        Ok(Some(row)) => row,
        Ok(None) => {
            println!("No data found for the specified criteria");
            return (
                StatusCode::OK,
                Json(json!({
                    "meta": {
                        "startTime": from_date,
                        "endTime": to_date,
                        "liquidityFees": "0",
                        "blockRewards": "0",
                        "earnings": "0",
                        "bondingEarnings": "0",
                        "liquidityEarnings": "0",
                        "avgNodeCount": "0",
                        "runePriceUSD": "0"
                    },
                    "intervals": []
                })),
            );
        },
        Err(e) => {
            println!("Meta query error: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Database query error: {}", e)
                })),
            );
        }
    };

    let mut meta_map = serde_json::Map::new();
    meta_map.insert("startTime".to_string(), Value::String(from_date.clone()));
    meta_map.insert("endTime".to_string(), Value::String(to_date.clone()));
    for (json_field, db_field) in fields.iter().zip(db_fields.iter()).skip(2) { // Skip startTime and endTime
        let value = get_string_value(&meta_row, db_field);
        meta_map.insert(json_field.to_string(), Value::String(value));
    }
    let meta = Value::Object(meta_map);

    let interval_format = match interval.as_str() {
        "day" => "%Y-%m-%d 00:00:00",
        "week" => "%Y-%v-1 00:00:00",
        "month" => "%Y-%m-01 00:00:00",
        "year" => "%Y-01-01 00:00:00",
        _ => "%Y-%m-%d %H:00:00",
    };

    let interval_query = if is_pool_specific {
        format!(
            "SELECT
             DATE_FORMAT(start_time, '{}') as interval_start,
             MIN(start_time) as start_time,
             MAX(end_time) as end_time,
             SUM(asset_liquidity_fees) as asset_liquidity_fees,
             SUM(rune_liquidity_fees) as rune_liquidity_fees,
             SUM(total_liquidity_fees_rune) as total_liquidity_fees_rune,
             SUM(earnings) as earnings,
             SUM(saver_earning) as saver_earning,
             SUM(rewards) as rewards
            FROM {}
            WHERE start_time >= ? AND end_time <= ?
            GROUP BY interval_start
            ORDER BY interval_start
            LIMIT ? OFFSET ?",
            interval_format, table_name
        )
    } else {
        format!(
            "SELECT
             DATE_FORMAT(start_time, '{}') as interval_start,
             MIN(start_time) as start_time,
             MAX(end_time) as end_time,
             SUM(liquidity_fees) as liquidity_fees,
             SUM(block_rewards) as block_rewards,
             SUM(earnings) as earnings,
             SUM(bonding_earnings) as bonding_earnings,
             SUM(liquidity_earning) as liquidity_earning,
             AVG(avg_node_count) as avg_node_count
            FROM {}
            WHERE start_time >= ? AND end_time <= ?
            GROUP BY interval_start
            ORDER BY interval_start
            LIMIT ? OFFSET ?",
            interval_format, table_name
        )
    };

    println!("Executing interval query");

    let interval_rows_result: Result<Vec<Row>, mysql::Error> = conn.exec(
        &interval_query,
        (from_date, to_date, limit, offset)
    );

    let interval_rows = match interval_rows_result {
        Ok(rows) => rows,
        Err(e) => {
            println!("Interval query error: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Database query error: {}", e)
                })),
            );
        }
    };

    let (interval_fields, interval_db_fields) = if is_pool_specific {
        (
            [
                "startTime", "endTime", "liquidityFees", "blockRewards", "earnings",
                "bondingEarnings", "liquidityEarnings", "avgNodeCount"
            ],
            [
                "start_time", "end_time", "total_liquidity_fees_rune", "rewards", "earnings",
                "saver_earning", "asset_liquidity_fees", "rune_liquidity_fees"
            ]
        )
    } else {
        (
            [
                "startTime", "endTime", "liquidityFees", "blockRewards", "earnings",
                "bondingEarnings", "liquidityEarnings", "avgNodeCount"
            ],
            [
                "start_time", "end_time", "liquidity_fees", "block_rewards", "earnings",
                "bonding_earnings", "liquidity_earning", "avg_node_count"
            ]
        )
    };

    let mut intervals = Vec::new();
    for row in interval_rows {
        let mut interval_map = serde_json::Map::new();
        for (json_field, db_field) in interval_fields.iter().zip(interval_db_fields.iter()) {
            interval_map.insert(
                json_field.to_string(),
                Value::String(get_string_value(&row, db_field))
            );
        }
        intervals.push(Value::Object(interval_map));
    }

    println!("Returning response with {} intervals", intervals.len());

    (
        StatusCode::OK,
        Json(json!({
            "meta": meta,
            "intervals": intervals
        })),
    )
}

// Handler for /depthandprice endpoint
async fn handle_depth_and_price(
    Query(params): Query<DepthAndPriceParams>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    println!("Received request for pool: {}", params.pool);

    let table_name = match map_pool_to_table(&params.pool) {
        Some(name) => {
            println!("Mapped to table: {}", name);
            name
        },
        None => {
            println!("Failed to map pool name: {}", params.pool);
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("Invalid pool name: {}", params.pool)
                })),
            );
        }
    };

    let from = params.from.unwrap_or_else(|| {
        let start_of_2020 = Utc.ymd(2020, 1, 1).and_hms(0, 0, 0).timestamp();
        println!("No 'from' parameter specified, defaulting to 2020-01-01");
        start_of_2020
    });

    let to = params.to.unwrap_or_else(|| {
        let end_of_2030 = Utc.ymd(2030, 12, 31).and_hms(23, 59, 59).timestamp();
        println!("No 'to' parameter specified, defaulting to 2030-12-31");
        end_of_2030
    });
    let interval = params.interval.unwrap_or_else(|| "hour".to_string());
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    let from_date = Utc.timestamp(from, 0).format("%Y-%m-%d %H:%M:%S").to_string();
    let to_date = Utc.timestamp(to, 0).format("%Y-%m-%d %H:%M:%S").to_string();

    println!("Query date range: {} to {}", from_date, to_date);

    let pool = &state.pool;
    let mut conn = match pool.get_conn() {
        Ok(conn) => conn,
        Err(e) => {
            println!("Database connection error: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Database connection error: {}", e)
                })),
            );
        }
    };

    // Meta query for depth and price data
    let meta_query = format!(
        "SELECT
         SUM(asset_depth) as asset_depth,
         SUM(rune_depths) as rune_depths,
         SUM(liquidity_units) as liquidity_units,
         SUM(members_count) as members_count,
         SUM(synth_units) as synth_units,
         AVG(luvi) as luvi,
         AVG(asset_price) as asset_price
        FROM {}
        WHERE start_time >= ? AND end_time <= ?",
        table_name
    );

    println!("Executing meta query with params: {} and {}", from_date, to_date);

    let meta_row_result: Result<Option<Row>, mysql::Error> = conn.exec_first(&meta_query, (from_date.clone(), to_date.clone()));

    let meta_row = match meta_row_result {
        Ok(Some(row)) => row,
        Ok(None) => {
            println!("No data found for the specified criteria");
            return (
                StatusCode::OK,
                Json(json!({
                    "meta": {
                        "startTime": from_date,
                        "endTime": to_date,
                        "priceShiftLoss": "0",
                        "luviIncrease": "0",
                        "startAssetDepth": "0",
                        "startRuneDepth": "0",
                        "startLPUnits": "0",
                        "startMemberCount": "0",
                        "startSynthUnits": "0",
                        "endAssetDepth": "0",
                        "endRuneDepth": "0",
                        "endLPUnits": "0",
                        "endMemberCount": "0",
                        "endSynthUnits": "0"
                    },
                    "intervals": []
                })),
            );
        },
        Err(e) => {
            println!("Meta query error: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Database query error: {}", e)
                })),
            );
        }
    };

    // Build meta object
    let mut meta_map = serde_json::Map::new();
    meta_map.insert("startTime".to_string(), Value::String(from_date.clone()));
    meta_map.insert("endTime".to_string(), Value::String(to_date.clone()));
    // Placeholder for priceShiftLoss and luviIncrease (not directly in schema, calculated if needed)
    meta_map.insert("priceShiftLoss".to_string(), Value::String("0".to_string())); // Placeholder
    meta_map.insert("luviIncrease".to_string(), Value::String(get_string_value(&meta_row, "luvi")));
    meta_map.insert("startAssetDepth".to_string(), Value::String(get_string_value(&meta_row, "asset_depth")));
    meta_map.insert("startRuneDepth".to_string(), Value::String(get_string_value(&meta_row, "rune_depths")));
    meta_map.insert("startLPUnits".to_string(), Value::String(get_string_value(&meta_row, "liquidity_units")));
    meta_map.insert("startMemberCount".to_string(), Value::String(get_string_value(&meta_row, "members_count")));
    meta_map.insert("startSynthUnits".to_string(), Value::String(get_string_value(&meta_row, "synth_units")));
    // Using same aggregated values for end* fields as a simplification (could be split with additional logic)
    meta_map.insert("endAssetDepth".to_string(), Value::String(get_string_value(&meta_row, "asset_depth")));
    meta_map.insert("endRuneDepth".to_string(), Value::String(get_string_value(&meta_row, "rune_depths")));
    meta_map.insert("endLPUnits".to_string(), Value::String(get_string_value(&meta_row, "liquidity_units")));
    meta_map.insert("endMemberCount".to_string(), Value::String(get_string_value(&meta_row, "members_count")));
    meta_map.insert("endSynthUnits".to_string(), Value::String(get_string_value(&meta_row, "synth_units")));
    let meta = Value::Object(meta_map);

    // Interval query
    let interval_format = match interval.as_str() {
        "day" => "%Y-%m-%d 00:00:00",
        "week" => "%Y-%v-1 00:00:00",
        "month" => "%Y-%m-01 00:00:00",
        "year" => "%Y-01-01 00:00:00",
        _ => "%Y-%m-%d %H:00:00",
    };

    let interval_query = format!(
        "SELECT
         DATE_FORMAT(start_time, '{}') as interval_start,
         MIN(start_time) as start_time,
         MAX(end_time) as end_time,
         SUM(asset_depth) as asset_depth,
         SUM(rune_depths) as rune_depths,
         SUM(liquidity_units) as liquidity_units,
         SUM(members_count) as members_count,
         SUM(synth_units) as synth_units,
         AVG(luvi) as luvi
        FROM {}
        WHERE start_time >= ? AND end_time <= ?
        GROUP BY interval_start
        ORDER BY interval_start
        LIMIT ? OFFSET ?",
        interval_format, table_name
    );

    println!("Executing interval query");

    let interval_rows_result: Result<Vec<Row>, mysql::Error> = conn.exec(
        &interval_query,
        (from_date, to_date, limit, offset)
    );

    let interval_rows = match interval_rows_result {
        Ok(rows) => rows,
        Err(e) => {
            println!("Interval query error: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Database query error: {}", e)
                })),
            );
        }
    };

    let mut intervals = Vec::new();
    for row in interval_rows {
        let mut interval_map = serde_json::Map::new();
        interval_map.insert("startTime".to_string(), Value::String(get_string_value(&row, "start_time")));
        interval_map.insert("endTime".to_string(), Value::String(get_string_value(&row, "end_time")));
        interval_map.insert("priceShiftLoss".to_string(), Value::String("0".to_string())); // Placeholder
        interval_map.insert("luviIncrease".to_string(), Value::String(get_string_value(&row, "luvi")));
        interval_map.insert("startAssetDepth".to_string(), Value::String(get_string_value(&row, "asset_depth")));
        interval_map.insert("startRuneDepth".to_string(), Value::String(get_string_value(&row, "rune_depths")));
        interval_map.insert("startLPUnits".to_string(), Value::String(get_string_value(&row, "liquidity_units")));
        interval_map.insert("startMemberCount".to_string(), Value::String(get_string_value(&row, "members_count")));
        interval_map.insert("startSynthUnits".to_string(), Value::String(get_string_value(&row, "synth_units")));
        intervals.push(Value::Object(interval_map));
    }

    println!("Returning response with {} intervals", intervals.len());

    (
        StatusCode::OK,
        Json(json!({
            "meta": meta,
            "intervals": intervals
        })),
    )
}

// Application state
struct AppState {
    pool: Pool,
}

// Index route handler
async fn index_handler() -> impl IntoResponse {
    "THORChain API Server"
}

// Start the API server
pub async fn start_api_server(mysql_pool: Pool) -> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(AppState {
        pool: mysql_pool,
    });

    let cors = tower_http::cors::CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any);

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/swaphistory", get(handle_swap_history))
        .route("/earninghistory", get(handle_earning_history))
        .route("/depthandprice", get(handle_depth_and_price)) // New endpoint
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    println!("Starting Axum API server on http://{}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}