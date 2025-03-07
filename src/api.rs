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

// Helper function to get string value from a database row
fn get_string_value(row: &Row, field: &str) -> String {
    // Try string first
    if let Some(Ok(value)) = row.get_opt::<String, _>(field) {
        return value;
    }

    // Try i64
    if let Some(Ok(value)) = row.get_opt::<i64, _>(field) {
        return value.to_string();
    }

    // Try f64
    if let Some(Ok(value)) = row.get_opt::<f64, _>(field) {
        return value.to_string();
    }

    // Default to empty string if not found or if type doesn't match
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

    // Map the pool name to the database table name
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
            )
        }
    };

    // Parse parameters with defaults
    let from = params.from.unwrap_or_else(|| {
        // Default to a much wider date range - starting from 2020
        let start_of_2020 = Utc.ymd(2020, 1, 1).and_hms(0, 0, 0).timestamp();
        println!("No 'from' parameter specified, defaulting to 2020-01-01");
        start_of_2020
    });

    let to = params.to.unwrap_or_else(|| {
        // Default to far future - end of 2030
        let end_of_2030 = Utc.ymd(2030, 12, 31).and_hms(23, 59, 59).timestamp();
        println!("No 'to' parameter specified, defaulting to 2030-12-31");
        end_of_2030
    });
    let interval = params.interval.unwrap_or_else(|| "hour".to_string());

    let limit = params.limit.unwrap_or(50);

    let offset = params.offset.unwrap_or(0);

    // Convert timestamps to dates for the query - use timestamp() instead of timestamp_opt()
    let from_date = Utc.timestamp(from, 0)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();

    let to_date = Utc.timestamp(to, 0)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();

    println!("Query date range: {} to {}", from_date, to_date);

    // Connect to the database
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
            )
        }
    };

    // Meta query - aggregate over the entire period
    let meta_query = format!(
        "SELECT
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

    // Check if the table exists and has data
    let check_query = format!("SELECT COUNT(*) as count FROM {}", table_name);
    let count_result: Result<Option<Row>, mysql::Error> = conn.query_first(&check_query);

    match count_result {
        Ok(Some(row)) => {
            let count: i64 = row.get("count").unwrap_or(0);
            println!("Table {} has {} rows", table_name, count);

            // Check date range
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

    match &meta_row_result {
        Ok(Some(_)) => println!("Meta query returned data"),
        Ok(None) => println!("Meta query returned no data"),
        Err(e) => println!("Meta query error: {}", e),
    }

    let meta_row = match meta_row_result {
        Ok(Some(row)) => row,
        Ok(None) => {
            // Return empty response if no data
            println!("No data found for the specified criteria");
            return (
                StatusCode::OK,
                Json(json!({
                    "meta": {},
                    "intervals": []
                })),
            )
        },
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Database query error: {}", e)
                })),
            )
        }
    };

    // Field mappings between JSON and database
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

    // Build meta object
    let mut meta_map = serde_json::Map::new();
    for (json_field, db_field) in fields.iter().zip(db_fields.iter()) {
        let value = get_string_value(&meta_row, db_field);
        meta_map.insert(
            json_field.to_string(),
            Value::String(value)
        );
    }

    let meta = Value::Object(meta_map);

    // Determine the interval grouping
    let interval_format = match interval.as_str() {
        "day" => "%Y-%m-%d 00:00:00",
        "week" => "%Y-%v-1 00:00:00", // Week format
        "month" => "%Y-%m-01 00:00:00",
        "year" => "%Y-01-01 00:00:00",
        _ => "%Y-%m-%d %H:00:00",     // Default to hour
    };

    // Intervals query - group by the specified interval
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

    match &interval_rows_result {
        Ok(rows) => println!("Interval query returned {} rows", rows.len()),
        Err(e) => println!("Interval query error: {}", e),
    }

    let interval_rows = match interval_rows_result {
        Ok(rows) => rows,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Database query error: {}", e)
                })),
            )
        }
    };

    // Process intervals
    let mut intervals = Vec::new();
    for row in interval_rows {
        let mut interval_map = serde_json::Map::new();

        // Add the basic interval fields
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

    // Return the response
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
    // Create the application state
    let state = Arc::new(AppState {
        pool: mysql_pool,
    });

    // Add CORS middleware
    let cors = tower_http::cors::CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any);

    // Create the router with our routes
    let app = Router::new()
        .route("/", get(index_handler))
        .route("/swaphistory", get(handle_swap_history))
        .layer(cors)
        .with_state(state);

    // Bind to an address
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    println!("Starting Axum API server on http://{}", addr);

    // Start the server using the correct method for Axum 0.6.x
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

// This can be removed as we'll use tokio::spawn directly in main.rs
// pub fn start_api_server_sync(pool: Pool) -> Result<(), Box<dyn std::error::Error>> {
//     // Create a runtime for running the async server
//     let runtime = tokio::runtime::Runtime::new()?;
//
//     // Spawn the async server
//     runtime.spawn(async move {
//         if let Err(e) = start_api_server(pool).await {
//             eprintln!("Error in API server: {}", e);
//         }
//     });
//
//     // Return immediately, server runs in background
//     Ok(())
// }