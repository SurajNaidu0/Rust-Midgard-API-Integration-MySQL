use chrono::{DateTime, Duration, Utc, TimeZone, Timelike};
use dotenvy::dotenv;
use mysql::*;
use mysql::prelude::*;
use reqwest;
use serde_json::Value;
use std::env;
use std::error::Error;
use std::thread;

// Add these helper functions at the top of your file
fn parse_float(value: &Value) -> f64 {
    value.as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .or_else(|| value.as_f64())
        .unwrap_or(0.0)
}

fn parse_int(value: &Value) -> i64 {
    value.as_str()
        .and_then(|s| s.parse::<i64>().ok())
        .or_else(|| value.as_i64())
        .unwrap_or(0)
}

fn pause_for(duration: Duration) {
    // Get the current time
    let start_time: DateTime<Utc> = Utc::now();

    // Calculate the end time
    let end_time = start_time + duration;

    // Wait until the end time is reached
    while Utc::now() < end_time {
        // Do nothing, just wait
    }
}

const DATA: &[(&str, &str)] = &[
    ("ETH.USDT-0XDAC17F958D2EE523A2206206994597C13D831EC7", "ETHUSDT0XDAC17F958D2EE523A2206206994594597C13D831EC7"),
    ("ETH.USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48", "ETHUSDC0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48"),
    ("BTC.BTC", "BTCBTC"),
    ("ETH.ETH", "ETHETH"),
];

#[derive(Debug)]
struct PoolData {
    start_time: String,
    end_time: String,
    asset_liquidity_fees: i64,
    earnings: i64,
    rewards: i64,
    rune_liquidity_fees: i64,
    saver_earning: i64,
    total_liquidity_fees_rune: i64,
}
struct TradeData {
    average_slip: f64,
    from_secured_average_slip: f64,
    from_secured_count: i64,
    from_secured_fees: i64,
    from_secured_volume: i64,
    from_secured_volume_usd: i64,
    from_trade_average_slip: f64,
    from_trade_count: i64,
    from_trade_fees: i64,
    from_trade_volume: i64,
    from_trade_volume_usd: i64,
    rune_price_usd: i64,
    synth_mint_average_slip: f64,
    synth_mint_count: i64,
    synth_mint_fees: i64,
    synth_mint_volume: i64,
    synth_mint_volume_usd: i64,
    synth_redeem_average_slip: f64,
    synth_redeem_count: i64,
    synth_redeem_fees: i64,
    synth_redeem_volume: i64,
    synth_redeem_volume_usd: i64,
    to_asset_average_slip: i64,
    to_asset_count: i64,
    to_asset_fees: i64,
    to_asset_volume: i64,
    to_asset_volume_usd: i64,
    to_rune_average_slip: f64,
    to_rune_count: i64,
    to_rune_fees: i64,
    to_rune_volume: i64,
    to_rune_volume_usd: i64,
    to_secured_average_slip: i64,
    to_secured_count: i64,
    to_secured_fees: i64,
    to_secured_volume: i64,
    to_secured_volume_usd: i64,
    to_trade_average_slip: f64,
    to_trade_count: i64,
    to_trade_fees: i64,
    to_trade_volume: i64,
    to_trade_volume_usd: i64,
    total_count: i64,
    total_fees: i64,
    total_volume: i64,
    total_volume_usd: i64,
}

struct MemberData {
    member_count: i64,
    member_unit: i64,
}

struct TotalEarning{
    avg_node_count: f64,
    block_rewards: i64,
    bonding_earnings: i64,
    earnings: i64,
    liquidity_earning: i64,
    liquidity_fees: i64
}

struct DepthsData {
    asset_depth: i64,
    asset_price: f64,
    asset_price_usd: f64,
    liquidity_units: i64,
    luvi: f64,
    members_count: i64,
    rune_depths: i64,
    synth_supply: i64,
    synth_units: i64,
    units: i64,
}

async fn fetch_and_store_pool_data(pool: &Pool, now: chrono::DateTime<Utc>) -> Result<(), Box<dyn Error>> {
    println!("Hello");
    let current_hour = now.with_minute(0).unwrap()
        .with_second(0).unwrap()
        .with_nanosecond(0).unwrap();
    let last_hour = current_hour - Duration::hours(1);

    let from_timestamp = last_hour.timestamp();
    let to_timestamp = current_hour.timestamp();

    //sleep logic
    pause_for(Duration::seconds(2));

    let url = format!(
        "https://midgard.ninerealms.com/v2/history/earnings?from={}&to={}",
        from_timestamp,
        to_timestamp
    );

    let client = reqwest::Client::new();
    let response = client.get(&url).send().await?;

    if !response.status().is_success() {
        println!("Error: HTTP {}", response.status());
        return Ok(());
    }


    let data: Value = response.json().await?;

    let url_member = format!(
        "https://midgard.ninerealms.com/v2/history/runepool?from={}&to={}",
        from_timestamp,
        to_timestamp
    );
    let client = reqwest::Client::new();
    let response_member = client.get(&url_member).send().await?;

    if !response_member.status().is_success() {
        println!("Error: HTTP {}", response_member.status());
        return Ok(());
    }
    let data_member:Value = response_member.json().await?;
    let member_entry = MemberData{
        member_count: parse_int(&data_member["intervals"][0]["count"]),
        member_unit: parse_int(&data_member["intervals"][0]["unit"])
    };
    let total_earning_entry = TotalEarning{
        avg_node_count: parse_float(&data["meta"]["avgNodeCount"]),
        block_rewards: parse_int(&data["meta"]["blockRewards"]),
        bonding_earnings: parse_int(&data["meta"]["bondingEarnings"]),
        earnings: parse_int(&data["meta"]["earnings"]),
        liquidity_earning: parse_int(&data["meta"]["liquidityEarnings"]),
        liquidity_fees: parse_int(&data["meta"]["liquidityFees"]),
    };
    println!("{}",data["meta"]["avgNodeCount"]);
    println!("{}",total_earning_entry.avg_node_count);
    let insert_query = format!(
        r"INSERT INTO RUNE_MEMBER (
        start_time,
        end_time,
        member_count,
        member_unit,
        avg_node_count,
        block_rewards,
        bonding_earnings,
        earnings,
        liquidity_earning,
        liquidity_fees
    ) VALUES (
        :start_time,
        :end_time,
        :member_count,
        :member_unit,
        :avg_node_count,
        :block_rewards,
        :bonding_earnings,
        :earnings,
        :liquidity_earning,
        :liquidity_fees
    )"
    );

    // Create the parameters for the query
    let params = params! {
    "start_time" => last_hour.format("%Y-%m-%d %H:%M:%S").to_string(),
    "end_time" => last_hour.format("%Y-%m-%d %H:%M:%S").to_string(),
    "member_count" => &member_entry.member_count,
    "member_unit" => &member_entry.member_unit,
        "avg_node_count" => &total_earning_entry.avg_node_count,
    "block_rewards" => &total_earning_entry.block_rewards,
    "bonding_earnings" => &total_earning_entry.bonding_earnings,
    "earnings" => &total_earning_entry.earnings,
    "liquidity_earning" => &total_earning_entry.liquidity_earning,
    "liquidity_fees" => &total_earning_entry.liquidity_fees,
};

    println!("{}", data_member);
    println!("{}", data_member["intervals"][0]["count"]);
    println!("{}", member_entry.member_count);

    // Execute the insert query
    let mut conn = pool.get_conn()?;
    conn.exec_drop(&insert_query, params)?;


    for &(key, value) in DATA {
        println!("{}: {}", key, value);

        // Fetch swap data
        let swap_url = format!(
            "https://midgard.ninerealms.com/v2/history/swaps?pool={}&from={}&to={}",
            key, from_timestamp, to_timestamp
        );
        pause_for(Duration::seconds(2));
        let swap_response = client.get(&swap_url).send().await?;
        if !swap_response.status().is_success() {
            println!("Failed to fetch swap data for {}", key);
            continue;
        }
        let swap_data: Value = swap_response.json().await?;

        // Fetch depth data
        let depth_url = format!(
            "https://midgard.ninerealms.com/v2/history/depths/{}?from={}&to={}",
            key, from_timestamp, to_timestamp
        );
        pause_for(Duration::seconds(2));

        let depth_response = client.get(&depth_url).send().await?;
        if !depth_response.status().is_success() {
            println!("Failed to fetch depth data for {}", key);
            continue;
        }
        let depth_data: Value = depth_response.json().await?;

        // Create data structures
        let swap_entry = TradeData {
            average_slip: parse_float(&swap_data["meta"]["averageSlip"]),
            from_secured_average_slip: parse_float(&swap_data["meta"]["fromSecuredAverageSlip"]),
            from_secured_count: parse_int(&swap_data["meta"]["fromSecuredCount"]),
            from_secured_fees: parse_int(&swap_data["meta"]["fromSecuredFees"]),
            from_secured_volume: parse_int(&swap_data["meta"]["fromSecuredVolume"]),
            from_secured_volume_usd: parse_int(&swap_data["meta"]["fromSecuredVolumeUSD"]),
            from_trade_average_slip: parse_float(&swap_data["meta"]["fromTradeAverageSlip"]),
            from_trade_count: parse_int(&swap_data["meta"]["fromTradeCount"]),
            from_trade_fees: parse_int(&swap_data["meta"]["fromTradeFees"]),
            from_trade_volume: parse_int(&swap_data["meta"]["fromTradeVolume"]),
            from_trade_volume_usd: parse_int(&swap_data["meta"]["fromTradeVolumeUSD"]),
            rune_price_usd: parse_int(&swap_data["meta"]["runePriceUSD"]),
            synth_mint_average_slip: parse_float(&swap_data["meta"]["synthMintAverageSlip"]),
            synth_mint_count: parse_int(&swap_data["meta"]["synthMintCount"]),
            synth_mint_fees: parse_int(&swap_data["meta"]["synthMintFees"]),
            synth_mint_volume: parse_int(&swap_data["meta"]["synthMintVolume"]),
            synth_mint_volume_usd: parse_int(&swap_data["meta"]["synthMintVolumeUSD"]),
            synth_redeem_average_slip: parse_float(&swap_data["meta"]["synthRedeemAverageSlip"]),
            synth_redeem_count: parse_int(&swap_data["meta"]["synthRedeemCount"]),
            synth_redeem_fees: parse_int(&swap_data["meta"]["synthRedeemFees"]),
            synth_redeem_volume: parse_int(&swap_data["meta"]["synthRedeemVolume"]),
            synth_redeem_volume_usd: parse_int(&swap_data["meta"]["synthRedeemVolumeUSD"]),
            to_asset_average_slip: parse_int(&swap_data["meta"]["toAssetAverageSlip"]),
            to_asset_count: parse_int(&swap_data["meta"]["toAssetCount"]),
            to_asset_fees: parse_int(&swap_data["meta"]["toAssetFees"]),
            to_asset_volume: parse_int(&swap_data["meta"]["toAssetVolume"]),
            to_asset_volume_usd: parse_int(&swap_data["meta"]["toAssetVolumeUSD"]),
            to_rune_average_slip: parse_float(&swap_data["meta"]["toRuneAverageSlip"]),
            to_rune_count: parse_int(&swap_data["meta"]["toRuneCount"]),
            to_rune_fees: parse_int(&swap_data["meta"]["toRuneFees"]),
            to_rune_volume: parse_int(&swap_data["meta"]["toRuneVolume"]),
            to_rune_volume_usd: parse_int(&swap_data["meta"]["toRuneVolumeUSD"]),
            to_secured_average_slip: parse_int(&swap_data["meta"]["toSecuredAverageSlip"]),
            to_secured_count: parse_int(&swap_data["meta"]["toSecuredCount"]),
            to_secured_fees: parse_int(&swap_data["meta"]["toSecuredFees"]),
            to_secured_volume: parse_int(&swap_data["meta"]["toSecuredVolume"]),
            to_secured_volume_usd: parse_int(&swap_data["meta"]["toSecuredVolumeUSD"]),
            to_trade_average_slip: parse_float(&swap_data["meta"]["toTradeAverageSlip"]),
            to_trade_count: parse_int(&swap_data["meta"]["toTradeCount"]),
            to_trade_fees: parse_int(&swap_data["meta"]["toTradeFees"]),
            to_trade_volume: parse_int(&swap_data["meta"]["toTradeVolume"]),
            to_trade_volume_usd: parse_int(&swap_data["meta"]["toTradeVolumeUSD"]),
            total_count: parse_int(&swap_data["meta"]["totalCount"]),
            total_fees: parse_int(&swap_data["meta"]["totalFees"]),
            total_volume: parse_int(&swap_data["meta"]["totalVolume"]),
            total_volume_usd: parse_int(&swap_data["meta"]["totalVolumeUSD"]),
        };

        let depths_entry = DepthsData {
            asset_depth: parse_int(&depth_data["intervals"][0]["assetDepth"]),
            asset_price: parse_float(&depth_data["intervals"][0]["assetPrice"]),
            asset_price_usd: parse_float(&depth_data["intervals"][0]["assetPriceUSD"]),
            liquidity_units: parse_int(&depth_data["intervals"][0]["liquidityUnits"]),
            luvi: parse_float(&depth_data["intervals"][0]["luvi"]),
            members_count: parse_int(&depth_data["intervals"][0]["membersCount"]),
            rune_depths: parse_int(&depth_data["intervals"][0]["runeDepth"]),
            synth_supply: parse_int(&depth_data["intervals"][0]["synthSupply"]),
            synth_units: parse_int(&depth_data["intervals"][0]["synthUnits"]),
            units: parse_int(&depth_data["intervals"][0]["units"]),
        };

        // Process pool data
        if let Some(pools) = data["meta"]["pools"].as_array() {
            if let Some(pool_data) = pools.iter().find(|p| p["pool"].as_str() == Some(key)) {
                let pool_entry = PoolData {
                    start_time: last_hour.format("%Y-%m-%d %H:%M:%S").to_string(),
                    end_time: current_hour.format("%Y-%m-%d %H:%M:%S").to_string(),
                    asset_liquidity_fees: parse_int(&pool_data["assetLiquidityFees"]),
                    earnings: parse_int(&pool_data["earnings"]),
                    rewards: parse_int(&pool_data["rewards"]),
                    rune_liquidity_fees: parse_int(&pool_data["runeLiquidityFees"]),
                    saver_earning: parse_int(&pool_data["saverEarning"]),
                    total_liquidity_fees_rune: parse_int(&pool_data["totalLiquidityFeesRune"]),
                };

                let mut conn = pool.get_conn()?;

                // Initial insert with basic pool data
                let insert_query = format!(
                    r"INSERT INTO {} (
                        start_time, end_time, asset_liquidity_fees, earnings, rewards,
                        rune_liquidity_fees, saver_earning, total_liquidity_fees_rune
                    ) VALUES (
                        :start_time, :end_time, :asset_fees, :earnings, :rewards,
                        :rune_fees, :saver_earning, :total_fees
                    )", value
                );

                let insert_params = params! {
                    "start_time" => &pool_entry.start_time,
                    "end_time" => &pool_entry.end_time,
                    "asset_fees" => &pool_entry.asset_liquidity_fees,
                    "earnings" => &pool_entry.earnings,
                    "rewards" => &pool_entry.rewards,
                    "rune_fees" => &pool_entry.rune_liquidity_fees,
                    "saver_earning" => &pool_entry.saver_earning,
                    "total_fees" => &pool_entry.total_liquidity_fees_rune
                };

                conn.exec_drop(&insert_query, insert_params)?;

                // Update with swap metrics - Part 1
                let update_swap_1 = format!(
                    r"UPDATE {} SET
        average_slip = :avg_slip,
        from_secured_average_slip = :secured_slip,
        from_secured_count = :secured_count,
        from_secured_fees = :secured_fees,
        from_secured_volume = :secured_vol,
        from_secured_volume_usd = :secured_vol_usd,
        from_trade_average_slip = :trade_slip,
        from_trade_count = :trade_count,
        from_trade_fees = :trade_fees,
        from_trade_volume = :trade_vol,
        from_trade_volume_usd = :trade_vol_usd,
        to_asset_average_slip = :to_asset_slip,
        to_asset_count = :to_asset_count,
        to_asset_fees = :to_asset_fees,
        to_asset_volume = :to_asset_vol,
        to_asset_volume_usd = :to_asset_vol_usd,
        to_rune_average_slip = :to_rune_slip,
        to_rune_count = :to_rune_count,
        to_rune_fees = :to_rune_fees,
        to_rune_volume = :to_rune_vol,
        to_rune_volume_usd = :to_rune_vol_usd,
        to_secured_average_slip = :to_secured_slip,
        to_secured_count = :to_secured_count,
        to_secured_fees = :to_secured_fees,
        to_secured_volume = :to_secured_vol,
        to_secured_volume_usd = :to_secured_vol_usd,
        to_trade_average_slip = :to_trade_slip,
        to_trade_count = :to_trade_count,
        to_trade_fees = :to_trade_fees,
        to_trade_volume = :to_trade_vol,
        to_trade_volume_usd = :to_trade_vol_usd,
        total_count = :total_count,
        total_fees = :total_fees,
        total_volume = :total_vol,
        total_volume_usd = :total_vol_usd
    WHERE start_time = :start_time AND end_time = :end_time",
                    value
                );

                let swap_params_1 = params! {
    "avg_slip" => &swap_entry.average_slip,
    "secured_slip" => &swap_entry.from_secured_average_slip,
    "secured_count" => &swap_entry.from_secured_count,
    "secured_fees" => &swap_entry.from_secured_fees,
    "secured_vol" => &swap_entry.from_secured_volume,
    "secured_vol_usd" => &swap_entry.from_secured_volume_usd,
    "trade_slip" => &swap_entry.from_trade_average_slip,
    "trade_count" => &swap_entry.from_trade_count,
    "trade_fees" => &swap_entry.from_trade_fees,
    "trade_vol" => &swap_entry.from_trade_volume,
    "trade_vol_usd" => &swap_entry.from_trade_volume_usd,
    "to_asset_slip" => &swap_entry.to_asset_average_slip,
    "to_asset_count" => &swap_entry.to_asset_count,
    "to_asset_fees" => &swap_entry.to_asset_fees,
    "to_asset_vol" => &swap_entry.to_asset_volume,
    "to_asset_vol_usd" => &swap_entry.to_asset_volume_usd,
    "to_rune_slip" => &swap_entry.to_rune_average_slip,
    "to_rune_count" => &swap_entry.to_rune_count,
    "to_rune_fees" => &swap_entry.to_rune_fees,
    "to_rune_vol" => &swap_entry.to_rune_volume,
    "to_rune_vol_usd" => &swap_entry.to_rune_volume_usd,
    "to_secured_slip" => &swap_entry.to_secured_average_slip,
    "to_secured_count" => &swap_entry.to_secured_count,
    "to_secured_fees" => &swap_entry.to_secured_fees,
    "to_secured_vol" => &swap_entry.to_secured_volume,
    "to_secured_vol_usd" => &swap_entry.to_secured_volume_usd,
    "to_trade_slip" => &swap_entry.to_trade_average_slip,
    "to_trade_count" => &swap_entry.to_trade_count,
    "to_trade_fees" => &swap_entry.to_trade_fees,
    "to_trade_vol" => &swap_entry.to_trade_volume,
    "to_trade_vol_usd" => &swap_entry.to_trade_volume_usd,
    "total_count" => &swap_entry.total_count,
    "total_fees" => &swap_entry.total_fees,
    "total_vol" => &swap_entry.total_volume,
    "total_vol_usd" => &swap_entry.total_volume_usd,
    "start_time" => &pool_entry.start_time,
    "end_time" => &pool_entry.end_time
};

                conn.exec_drop(&update_swap_1, swap_params_1)?;use chrono::{DateTime, Duration, Utc, TimeZone, Timelike};

                // Update with swap metrics - Part 2
                let update_swap_2 = format!(
                    r"UPDATE {} SET
        rune_price_usd = :rune_price,
        synth_mint_average_slip = :mint_slip,
        synth_mint_count = :mint_count,
        synth_mint_fees = :mint_fees,
        synth_mint_volume = :mint_vol,
        synth_mint_volume_usd = :mint_vol_usd,
        synth_redeem_average_slip = :redeem_slip,
        synth_redeem_count = :redeem_count,
        synth_redeem_fees = :redeem_fees,
        synth_redeem_volume = :redeem_vol,
        synth_redeem_volume_usd = :redeem_vol_usd
    WHERE start_time = :start_time AND end_time = :end_time",
                    value
                );

                let swap_params_2 = params! {
    "rune_price" => &swap_entry.rune_price_usd,
    "mint_slip" => &swap_entry.synth_mint_average_slip,
    "mint_count" => &swap_entry.synth_mint_count,
    "mint_fees" => &swap_entry.synth_mint_fees,
    "mint_vol" => &swap_entry.synth_mint_volume,
    "mint_vol_usd" => &swap_entry.synth_mint_volume_usd,
    "redeem_slip" => &swap_entry.synth_redeem_average_slip,
    "redeem_count" => &swap_entry.synth_redeem_count,
    "redeem_fees" => &swap_entry.synth_redeem_fees,
    "redeem_vol" => &swap_entry.synth_redeem_volume,
    "redeem_vol_usd" => &swap_entry.synth_redeem_volume_usd,
    "start_time" => &pool_entry.start_time,
    "end_time" => &pool_entry.end_time
};

                conn.exec_drop(&update_swap_2, swap_params_2)?;

                // Update with depth metrics
                let update_depths = format!(
                    r"UPDATE {} SET
                        asset_depth = :asset_depth,
                        asset_price = :asset_price,
                        asset_price_usd = :asset_price_usd,
                        liquidity_units = :liq_units,
                        luvi = :luvi,
                        members_count = :members_count,
                        rune_depths = :rune_depths,
                        synth_supply = :synth_supply,
                        synth_units = :synth_units,
                        units = :units
                    WHERE start_time = :start_time AND end_time = :end_time",
                    value
                );

                let depth_params = params! {
                    "asset_depth" => &depths_entry.asset_depth,
                    "asset_price" => &depths_entry.asset_price,
                    "asset_price_usd" => &depths_entry.asset_price_usd,
                    "liq_units" => &depths_entry.liquidity_units,
                    "luvi" => &depths_entry.luvi,
                    "members_count" => &depths_entry.members_count,
                    "rune_depths" => &depths_entry.rune_depths,
                    "synth_supply" => &depths_entry.synth_supply,
                    "synth_units" => &depths_entry.synth_units,
                    "units" => &depths_entry.units,
                    "start_time" => &pool_entry.start_time,
                    "end_time" => &pool_entry.end_time
                };

                conn.exec_drop(&update_depths, depth_params)?;

                println!("Successfully stored pool data for timestamp: {} poolname: {}", current_hour, key);
            } else {
                println!("Pool {} not found in the current time period", key);
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL not found in .env file");
    let opts = Opts::from_url(&database_url).expect("Invalid database URL");

    let pool = Pool::new(opts)?;

    // Create table if it doesn't exist
    let mut conn = pool.get_conn()?;
    for &(key, value) in DATA {
        println!("{}: {}", key, value);
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
    let query = format!(
        r"CREATE TABLE IF NOT EXISTS RUNE_MEMBER (
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
    )"
    );
    conn.query_drop(&query)?;

    //*********Logic to get data from past 2 months***********
    let now = Utc::now();
    let two_months_ago = now - Duration::days(60);

    let mut current_time = two_months_ago;

    while current_time <= now {
        fetch_and_store_pool_data(&pool, current_time).await?;
        current_time = current_time + Duration::hours(1);
    }
    // fetch_and_store_pool_data(&pool).await?;
    //************Logic to get info for every one hours*************
    // loop {
    //     let now = Utc::now();
    //     if let Err(e) = fetch_and_store_pool_data(&pool, now) {
    //         eprintln!("Error fetching and storing pool data: {}", e);
    //     }
    //
    //     // Sleep for one hour
    //     pause_for(Duration::seconds(3600));
    // }

    Ok(())
}
