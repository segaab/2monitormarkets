import streamlit as st
import pandas as pd
from supabase_client import get_supabase_client
import logging
import numpy as np # Import numpy for percentile calculation
from pytz import timezone
import pytz


# Configure logging - Set level to INFO for normal operation, DEBUG for detailed calculation logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
TARGET_ASSETS = [
    "BITCOIN - CHICAGO MERCANTILE EXCHANGE",
    "MICRO BITCOIN - CHICAGO MERCANTILE EXCHANGE",
    "MICRO ETHER - CHICAGO MERCANTILE EXCHANGE",
    "GOLD - COMMODITY EXCHANGE INC.",
    "SILVER - COMMODITY EXCHANGE INC.",
    "WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE",
    "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE",
    "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE",
    "EURO FX - CHICAGO MERCANTILE EXCHANGE",
    "U.S. DOLLAR INDEX - ICE FUTURES U.S.",
    "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE",
    "DOW JONES U.S. REAL ESTATE IDX - CHICAGO BOARD OF TRADE",
    "E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
    "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE",
    "NIKKEI STOCK AVERAGE - CHICAGO MERCANTILE EXCHANGE"
]

# Define trader categories
TRADER_CATEGORIES = ["noncomm", "comm", "nonrept"]

# Mapping of asset names to TradingView URLs
TRADINGVIEW_URLS = {
    "GOLD - COMMODITY EXCHANGE INC.": "https://www.tradingview.com/chart/jMGev8A9/?symbol=OANDA%3AXAUUSD",
    "EURO FX - CHICAGO MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=OANDA%3AEURUSD",
    "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=OANDA%3AAUDUSD",
    "BITCOIN - CHICAGO MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=BINANCE%3ABTCUSDT",
    "MICRO BITCOIN - CHICAGO MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=CME%3AMBT1%21",
    "MICRO ETHER - CHICAGO MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=CME%3AMET1%21",
    "SILVER - COMMODITY EXCHANGE INC.": "https://www.tradingview.com/chart/jMGev8A9/?symbol=OANDA%3AXAGUSD",
    "WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=BLACKBULL%3AWTI",
    "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=OANDA%3AUSDJPY",
    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=OANDA%3AUSDCAD",
    "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=OANDA%3AGBPUSD",
    "U.S. DOLLAR INDEX - ICE FUTURES U.S.": "https://www.tradingview.com/chart/jMGev8A9/?symbol=TVC%3ADXY",
    "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=OANDA%3ANZDUSD",
    "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=OANDA%3AUSDCHF",
    "DOW JONES U.S. REAL ESTATE IDX - CHICAGO BOARD OF TRADE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=BLACKBULL%3AUS30",
    "E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=CME_MINI%3AES1%21",
    "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=CME_MINI%3ANQ1%21",
    "NIKKEI STOCK AVERAGE - CHICAGO MERCANTILE EXCHANGE": "https://www.tradingview.com/chart/jMGev8A9/?symbol=VANTAGE%3ANIKKEI225"
}


# --- Helper Functions ---

@st.cache_data(ttl=3600) # Cache data for 1 hour to avoid re-fetching frequently
def fetch_historical_reports(_supabase_client, asset_name, limit=52):
    """Fetches historical COT reports for a given asset from Supabase."""
    logging.info(f"Attempting to fetch last {limit} reports for asset: {asset_name} for historical analysis.")
    try:
        # Use the _supabase_client parameter
        response = _supabase_client.table("cot_reports").select("*").eq("market_and_exchange_names", asset_name).order("report_date", desc=True).limit(limit).execute()
        fetched_data = response.data

        if fetched_data is not None and isinstance(fetched_data, list):
             logging.info(f"Successfully fetched {len(fetched_data)} historical records for {asset_name}.")
             return fetched_data
        else:
             logging.warning(f"Fetch successful for {asset_name} but no historical data was returned in the expected format. Received type for .data: {type(fetched_data)}. Returning empty list.")
             return []

    except Exception as e:
        logging.exception(f"Exception occurred while fetching historical reports for {asset_name}: {e}")
        # Don't show error on every historical fetch, just log it.
        return None # Indicate a fetch error

@st.cache_data(ttl=300) # Cache latest reports for 5 minutes
def fetch_latest_two_reports(_supabase_client, asset_name):
    """Fetches the latest two COT reports for a given asset from Supabase."""
    logging.info(f"Attempting to fetch latest two reports for asset: {asset_name} for current analysis.")
    try:
        # Use the _supabase_client parameter
        response = _supabase_client.table("cot_reports").select("*").eq("market_and_exchange_names", asset_name).order("report_date", desc=True).limit(2).execute()
        fetched_data = response.data

        if fetched_data is not None and isinstance(fetched_data, list):
             logging.info(f"Successfully fetched latest reports for {asset_name}. Received {len(fetched_data)} records.")
             return fetched_data # Return the list of records
        else:
             logging.warning(f"Fetch successful for {asset_name} but no latest data was returned in the expected format. Received type for .data: {type(fetched_data)}. Returning empty list.")
             return [] # Return an empty list to indicate no data

    except Exception as e:
        logging.exception(f"Exception occurred while fetching latest reports for {asset_name}: {e}")
        st.error(f"Fetch: There was an error fetching latest data for {asset_name}. Details: {e}")
        return None # Return None to indicate a fetch error


def calculate_net_position_ratio(long, short):
    """Calculates the ratio (Long - Short) / (Long + Short), handling division by zero."""
    total_positions = long + short
    if total_positions == 0:
        return 0.0
    ratio = (long - short) / total_positions
    return ratio

def calculate_historical_net_ratio_changes_by_group(reports, asset_name, asset_group_direction_thresholds):
    """Calculates the net position ratio change for each report compared to the previous, separated into positive/negative, using asset-specific thresholds."""
    if not isinstance(reports, list) or len(reports) < 2:
        return {category: {'positive': [], 'negative': []} for category in TRADER_CATEGORIES}

    changes_by_group = {category: {'positive': [], 'negative': []} for category in TRADER_CATEGORIES}
    # Reports are ordered descending by date, so iterate from the second report
    for i in range(1, len(reports)):
        latest_report = reports[i-1] # The more recent report
        previous_report = reports[i] # The older report

        for category in TRADER_CATEGORIES:
             latest_long = latest_report.get(f"{category}_positions_long_all", 0)
             latest_short = latest_report.get(f"{category}_positions_short_all", 0)
             previous_long = previous_report.get(f"{category}_positions_long_all", 0)
             previous_short = previous_report.get(f"{category}_positions_short_all", 0)

             latest_ratio = calculate_net_position_ratio(latest_long, latest_short)
             previous_ratio = calculate_net_position_ratio(previous_long, previous_short)

             net_ratio_change = latest_ratio - previous_ratio

             # Get thresholds for the current asset and category
             positive_threshold = asset_group_direction_thresholds[asset_name][category]['positive']
             negative_threshold = asset_group_direction_thresholds[asset_name][category]['negative']

             if net_ratio_change > positive_threshold:
                 changes_by_group[category]['positive'].append(net_ratio_change)
             elif net_ratio_change < -negative_threshold:
                 changes_by_group[category]['negative'].append(net_ratio_change)

    return changes_by_group


def calculate_latest_net_ratio_changes(reports):
    """Calculates the net position ratio change for the latest two reports."""
    if not isinstance(reports, list) or len(reports) < 2:
        logging.warning("Less than two valid reports available for latest change calculation.")
        return None

    latest_report = reports[0]
    previous_report = reports[1]

    changes = {}
    for category in TRADER_CATEGORIES:
        latest_long = latest_report.get(f"{category}_positions_long_all", 0)
        latest_short = latest_report.get(f"{category}_positions_short_all", 0)
        previous_long = previous_report.get(f"{category}_positions_long_all", 0)
        previous_short = previous_report.get(f"{category}_positions_short_all", 0)

        latest_ratio = calculate_net_position_ratio(latest_long, latest_short)
        previous_ratio = calculate_net_position_ratio(previous_long, previous_short)

        net_ratio_change = latest_ratio - previous_ratio

        changes[f"{category}_net_ratio_change"] = net_ratio_change

    return changes

# --- Streamlit App ---
def main():
    st.title("Commitment of Traders (COT) Analysis")
    logging.info("Streamlit app started.")

    # Initialize Supabase client
    logging.info("Initializing Supabase client.")
    supabase_client = get_supabase_client()
    if not supabase_client:
        st.error("Failed to initialize Supabase client.")
        logging.error("Supabase client initialization failed.")
        return
    logging.info("Supabase client initialized successfully.")

    # --- Calculate Individual Asset and Group Thresholds (40th percentile) ---
    logging.info("Calculating individual asset and group net change thresholds (40th percentile)...")
    # Nested dictionary to store thresholds per asset, group, and direction (positive/negative)
    asset_group_direction_thresholds = {}

    for asset_name in TARGET_ASSETS:
        asset_group_direction_thresholds[asset_name] = {} # Initialize nested dictionary for the asset
        historical_reports = fetch_historical_reports(supabase_client, asset_name, limit=52)

        if historical_reports and len(historical_reports) >= 2:
            historical_changes_by_group = calculate_historical_net_ratio_changes_by_group(historical_reports, asset_name, asset_group_direction_thresholds)

            for category in TRADER_CATEGORIES:
                 asset_group_direction_thresholds[asset_name][category] = {} # Initialize direction dictionary
                 positive_changes = historical_changes_by_group.get(category, {}).get('positive', [])
                 negative_changes = historical_changes_by_group.get(category, {}).get('negative', [])

                 # Calculate 40th percentile for positive changes
                 if positive_changes:
                     positive_threshold = np.percentile(positive_changes, 40)
                     asset_group_direction_thresholds[asset_name][category]['positive'] = positive_threshold
                     logging.info(f"Calculated 40th percentile positive threshold for {asset_name} - {category}: {positive_threshold:.6f}")
                 else:
                     asset_group_direction_thresholds[asset_name][category]['positive'] = 0 # Set threshold to 0 if no historical positive changes
                     logging.warning(f"No historical positive net changes found for {asset_name} - {category} to calculate threshold.")

                 # Calculate 40th percentile for absolute negative changes
                 if negative_changes:
                     # Calculate percentile on absolute values, but store as positive threshold
                     negative_threshold = np.percentile([abs(change) for change in negative_changes], 40)
                     asset_group_direction_thresholds[asset_name][category]['negative'] = negative_threshold
                     logging.info(f"Calculated 40th percentile negative threshold for {asset_name} - {category}: {negative_threshold:.6f}")
                 else:
                     asset_group_direction_thresholds[asset_name][category]['negative'] = 0 # Set threshold to 0 if no historical negative changes
                     logging.warning(f"No historical negative net changes found for {asset_name} - {category} to calculate threshold.")

        else:
            # Set thresholds to 0 for all groups and directions if not enough historical reports for the asset
            for category in TRADER_CATEGORIES:
                asset_group_direction_thresholds[asset_name][category] = {'positive': 0, 'negative': 0}
            logging.warning(f"Not enough historical reports found for {asset_name} to calculate thresholds for all groups and directions.")

    # Display a message about threshold calculation in sidebar
    st.sidebar.header("Filtering Thresholds")
    has_any_threshold = False
    for asset_thresholds in asset_group_direction_thresholds.values():
        for group_thresholds in asset_thresholds.values():
            if group_thresholds.get('positive', 0) > 0 or group_thresholds.get('negative', 0) > 0:
                has_any_threshold = True
                break
        if has_any_threshold:
            break

    if has_any_threshold:
         st.sidebar.info("Thresholds are calculated individually for each asset, trader group, and direction (positive/negative) based on their last 52 reports (40th percentile).")
    else:
         st.sidebar.warning("Could not calculate thresholds for any asset/group/direction combination. Filtering is disabled.")


    # --- Filtering Options ---
    st.sidebar.header("Filter Assets by Significant Change (AND logic)")
    filter_noncomm_long = st.sidebar.checkbox("Non-Commercial Significant Net Long Change")
    filter_noncomm_short = st.sidebar.checkbox("Non-Commercial Significant Net Short Change")
    filter_comm_long = st.sidebar.checkbox("Commercial Significant Net Long Change")
    filter_comm_short = st.sidebar.checkbox("Commercial Significant Net Short Change")
    filter_nonrept_long = st.sidebar.checkbox("Non-Reportable Significant Net Long Change")
    filter_nonrept_short = st.sidebar.checkbox("Non-Reportable Significant Net Short Change")


    # --- Display Analysis for Filtered Assets ---
    

    displayed_assets_count = 0

    # Iterate over each target asset and display its analysis
    for asset_name in TARGET_ASSETS:
        logging.info(f"Processing analysis for {asset_name}")

        # Get the individual group and direction thresholds for this asset
        group_direction_thresholds = asset_group_direction_thresholds.get(asset_name, {})

        # Fetch the latest two reports for the asset (for current analysis)
        reports = fetch_latest_two_reports(supabase_client, asset_name)

        # Calculate latest net ratio changes
        latest_changes = None
        if reports is not None and len(reports) >= 2:
             latest_changes = calculate_latest_net_ratio_changes(reports)
             logging.debug(f"Latest calculated changes for {asset_name}: {latest_changes}")

        # Determine if the asset should be displayed based on filters and latest changes (AND logic)
        display_asset = False

        # Check if any filter is active
        any_filter_active = filter_noncomm_long or filter_noncomm_short or \
                            filter_comm_long or filter_comm_short or \
                            filter_nonrept_long or filter_nonrept_short

        # If no filters are active, display all assets with sufficient recent data
        if not any_filter_active:
             if reports is not None and len(reports) >= 2:
                  display_asset = True
                  logging.info(f"No filters active, displaying {asset_name}.")
        # If filters are active, apply AND logic
        elif latest_changes and group_direction_thresholds: # Proceed only if latest changes available and thresholds exist for this asset
             noncomm_change = latest_changes.get('noncomm_net_ratio_change', 0)
             comm_change = latest_changes.get('comm_net_ratio_change', 0)
             nonrept_change = latest_changes.get('nonrept_net_ratio_change', 0)

             # Get group and direction-specific thresholds
             noncomm_pos_threshold = group_direction_thresholds.get('noncomm', {}).get('positive', 0)
             noncomm_neg_threshold = group_direction_thresholds.get('noncomm', {}).get('negative', 0)
             comm_pos_threshold = group_direction_thresholds.get('comm', {}).get('positive', 0)
             comm_neg_threshold = group_direction_thresholds.get('comm', {}).get('negative', 0)
             nonrept_pos_threshold = group_direction_thresholds.get('nonrept', {}).get('positive', 0)
             nonrept_neg_threshold = group_direction_thresholds.get('nonrept', {}).get('negative', 0)


             # Assume the asset passes all selected filters initially
             passes_all_selected_filters = True

             # Check each filter individually if it's active, and apply AND logic
             if filter_noncomm_long:
                 # Criteria: Non-Commercial net change is positive AND greater than its positive threshold
                 if not (noncomm_change > noncomm_pos_threshold and noncomm_pos_threshold > 0):
                     passes_all_selected_filters = False
             if filter_noncomm_short:
                 # Criteria: Non-Commercial net change is negative AND less than negative of its negative threshold
                 if not (noncomm_change < -noncomm_neg_threshold and noncomm_neg_threshold > 0):
                     passes_all_selected_filters = False
             if filter_comm_long:
                 # Criteria: Commercial net change is positive AND greater than its positive threshold
                 if not (comm_change > comm_pos_threshold and comm_pos_threshold > 0):
                     passes_all_selected_filters = False
             if filter_comm_short:
                 # Criteria: Commercial net change is negative AND less than negative of its negative threshold
                 if not (comm_change < -comm_neg_threshold and comm_neg_threshold > 0):
                     passes_all_selected_filters = False
             if filter_nonrept_long:
                 # Criteria: Non-Reportable net change is positive AND greater than its positive threshold
                 if not (nonrept_change > nonrept_pos_threshold and nonrept_pos_threshold > 0):
                     passes_all_selected_filters = False
             if filter_nonrept_short:
                 # Criteria: Non-Reportable net change is negative AND less than negative of its negative threshold
                 if not (nonrept_change < -nonrept_neg_threshold and nonrept_neg_threshold > 0):
                     passes_all_selected_filters = False

             # If the asset passed all selected filters, mark it for display
             if passes_all_selected_filters:
                  display_asset = True
                  logging.info(f"{asset_name} passed all active filters.")
             else:
                  logging.info(f"{asset_name} did not pass all active filters.")

        else: # Handle cases where filters are active but latest_changes not available or no valid thresholds
             logging.info(f"Skipping filtering for {asset_name} due to missing data or thresholds.")
             pass # Asset will not be displayed as display_asset is still False


        # Display the analysis if the asset should be displayed and latest changes are available
        if display_asset and latest_changes:
            displayed_assets_count += 1
            st.subheader(asset_name)
            st.write(f"**Non-Commercial Ratio Change:** {latest_changes['noncomm_net_ratio_change'] * 100:.2f}%")
            st.write(f"**Commercial Ratio Change:** {latest_changes['comm_net_ratio_change'] * 100:.2f}%")
            st.write(f"**Non-Reportable Ratio Change:** {latest_changes['nonrept_net_ratio_change'] * 100:.2f}%")
            # Optional: Display the individual asset's and group's thresholds here for reference
            
            # Add TradingView link
            tradingview_url = TRADINGVIEW_URLS.get(asset_name)
            if tradingview_url:
                 st.markdown(f"[View on TradingView]({tradingview_url})")
            st.markdown("---") # Add a separator


    if displayed_assets_count == 0 and any_filter_active:
         st.info("No assets matched the selected filter criteria.")
    elif displayed_assets_count == 0 and has_any_threshold: # Check if any threshold was calculated
        st.info("No assets to display. Ensure you have at least two recent reports per asset and enough historical data (at least 2 reports) to calculate thresholds, or adjust your filters.")
    elif displayed_assets_count == 0:
         st.info("No assets to display. Ensure you have at least two recent reports per asset and enough historical data (at least 2 reports) to calculate thresholds.")

def fetch_latest_cot_data():
    """Fetches the latest and previous COT reports for all target assets and computes net position ratio changes."""
    logging.info("Starting fetch_latest_cot_data()...")

    supabase_client = get_supabase_client()
    if not supabase_client:
        logging.error("Failed to initialize Supabase client.")
        return pd.DataFrame(), pd.DataFrame()

    results = []

    for asset_name in TARGET_ASSETS:
        reports = fetch_latest_two_reports(supabase_client, asset_name)
        if reports and len(reports) >= 2:
            changes = calculate_latest_net_ratio_changes(reports)
            if changes:
                results.append({
                    "market_and_exchange_names": asset_name,
                    **changes
                })

    df = pd.DataFrame(results)
    return df, None  # No previous_df needed for dashboard use

def compute_net_position_ratios(df_latest, df_prev):
    """Combines two COT snapshots into a DataFrame of net ratio changes per trader category."""
    combined = []

    if df_latest is None or df_prev is None:
        return pd.DataFrame()

    latest_assets = df_latest['market_and_exchange_names'].unique()

    for asset_name in latest_assets:
        latest = df_latest[df_latest['market_and_exchange_names'] == asset_name]
        prev = df_prev[df_prev['market_and_exchange_names'] == asset_name]

        if not latest.empty and not prev.empty:
            latest = latest.iloc[0]
            prev = prev.iloc[0]

            result = {
                "market_and_exchange_names": asset_name,
                "noncomm_net_ratio_change": calculate_net_position_ratio(
                    latest.get("noncomm_positions_long_all", 0),
                    latest.get("noncomm_positions_short_all", 0)
                ) - calculate_net_position_ratio(
                    prev.get("noncomm_positions_long_all", 0),
                    prev.get("noncomm_positions_short_all", 0)
                ),
                "comm_net_ratio_change": calculate_net_position_ratio(
                    latest.get("comm_positions_long_all", 0),
                    latest.get("comm_positions_short_all", 0)
                ) - calculate_net_position_ratio(
                    prev.get("comm_positions_long_all", 0),
                    prev.get("comm_positions_short_all", 0)
                ),
                "nonrep_net_ratio_change": calculate_net_position_ratio(
                    latest.get("nonrept_positions_long_all", 0),
                    latest.get("nonrept_positions_short_all", 0)
                ) - calculate_net_position_ratio(
                    prev.get("nonrept_positions_long_all", 0),
                    prev.get("nonrept_positions_short_all", 0)
                )
            }

            combined.append(result)

    return pd.DataFrame(combined)

if __name__ == "__main__":
    main()