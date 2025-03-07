mod MidgardData;
mod api;

use chrono::{DateTime, Duration, Utc};
use dotenvy::dotenv;
use mysql::*;
use mysql::prelude::*;
use std::env;
use std::error::Error;
use crate::MidgardData::{get_last_timestamp, backfill_missing_data, run_hourly_update};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Load environment variables
    dotenv().ok();
    println!("Starting THORChain data collector and API...");

    // Set up database connection
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL not found in .env file");
    let opts = Opts::from_url(&database_url).expect("Invalid database URL");
    let pool = Pool::new(opts)?;

    println!("Connected to database successfully");

    // Create tables if they don't exist
    create_tables(&pool)?;

    // Get the last timestamp from the database
    println!("Checking for existing data...");
    let last_timestamp = get_last_timestamp(&pool).await?;

    match last_timestamp {
        Some(timestamp) => {
            println!("Found existing data up to {}. Will backfill from this point.",
                     timestamp.format("%Y-%m-%d %H:%M:%S"));
        },
        None => {
            println!("No existing data found. Will backfill last 60 days of data.");
        }
    }

    // Start API server as a background task within the existing runtime
    println!("Starting API server...");
    let api_pool = pool.clone();
    tokio::spawn(async move {
        if let Err(e) = api::start_api_server(api_pool).await {
            eprintln!("Error starting API server: {}", e);
        }
    });

    // Run the collector task in the main thread
    backfill_missing_data(&pool, last_timestamp).await?;
    println!("Data backfill complete!");

    println!("Starting hourly update service...");
    println!("Press Ctrl+C to stop the service");
    run_hourly_update(&pool).await?;

    Ok(())
}

// Helper function to create tables
fn create_tables(pool: &Pool) -> Result<(), Box<dyn Error>> {
    use crate::MidgardData::DATA;

    let mut conn = pool.get_conn()?;
    for &(key, value) in DATA {
        println!("Creating table for {}: {}", key, value);
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (id BIGINT AUTO_INCREMENT PRIMARY KEY,
            start_time DATETIME,
            end_time DATETIME,
            asset_liquidity_fees BIGINT,
            earnings BIGINT,
            rewards BIGINT,
            rune_liquidity_fees BIGINT,
            saver_earning BIGINT,
            total_liquidity_fees_rune BIGINT,
            average_slip DOUBLE,
            from_secured_average_slip DOUBLE,
            from_secured_count BIGINT,
            from_secured_fees BIGINT,
            from_secured_volume BIGINT,
            from_secured_volume_usd BIGINT,
            from_trade_average_slip DOUBLE,
            from_trade_count BIGINT,
            from_trade_fees BIGINT,
            from_trade_volume BIGINT,
            from_trade_volume_usd BIGINT,
            rune_price_usd BIGINT,
            synth_mint_average_slip DOUBLE,
            synth_mint_count BIGINT,
            synth_mint_fees BIGINT,
            synth_mint_volume BIGINT,
            synth_mint_volume_usd BIGINT,
            synth_redeem_average_slip DOUBLE,
            synth_redeem_count BIGINT,
            synth_redeem_fees BIGINT,
            synth_redeem_volume BIGINT,
            synth_redeem_volume_usd BIGINT,
            to_asset_average_slip BIGINT,
            to_asset_count BIGINT,
            to_asset_fees BIGINT,
            to_asset_volume BIGINT,
            to_asset_volume_usd BIGINT,
            to_rune_average_slip DOUBLE,
            to_rune_count BIGINT,
            to_rune_fees BIGINT,
            to_rune_volume BIGINT,
            to_rune_volume_usd BIGINT,
            to_secured_average_slip BIGINT,
            to_secured_count BIGINT,
            to_secured_fees BIGINT,
            to_secured_volume BIGINT,
            to_secured_volume_usd BIGINT,
            to_trade_average_slip DOUBLE,
            to_trade_count BIGINT,
            to_trade_fees BIGINT,
            to_trade_volume BIGINT,
            to_trade_volume_usd BIGINT,
            total_count BIGINT,
            total_fees BIGINT,
            total_volume BIGINT,
            total_volume_usd BIGINT,
            asset_depth BIGINT,
            asset_price DOUBLE,
            asset_price_usd DOUBLE,
            liquidity_units BIGINT,
            luvi DOUBLE,
            members_count BIGINT,
            rune_depths BIGINT,
            synth_supply BIGINT,
            synth_units BIGINT,
            units BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            value
        );
        conn.query_drop(query)?;
    }

    let query = r"CREATE TABLE IF NOT EXISTS RUNE_MEMBER (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        start_time DATETIME,
        end_time DATETIME,
        member_count BIGINT,
        member_unit BIGINT,
        avg_node_count DOUBLE,
        block_rewards BIGINT,
        bonding_earnings BIGINT,
        earnings BIGINT,
        liquidity_earning BIGINT,
        liquidity_fees BIGINT
    )";
    conn.query_drop(query)?;

    println!("Database tables created/verified");
    Ok(())
}
