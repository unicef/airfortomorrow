#!/usr/bin/env python3
"""
OpenAQ Real-time Data Collection using Python Client

This script collects real-time air quality data from OpenAQ using the Python client library.
It fetches data for each location for the past 2 days by default.
"""

import time
import argparse
import os
from datetime import datetime, timedelta
import pandas as pd
from pandas import json_normalize
from pathlib import Path
from openaq import OpenAQ
from dotenv import load_dotenv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from src.data_collectors.openaq_collector import OpenAQCollector

# Configuration for parallel processing
MAX_WORKERS = 2  # Reduced from 4 - Number of concurrent workers
BATCH_SIZE = 10   # Reduced from 15 - Sensors per batch  
MEASUREMENTS_LIMIT = 500
MAX_SENSOR_RETRIES = 3
FALLBACK_RATE_LIMIT_WAIT_SECONDS = 60.0
RESET_JITTER_SECONDS = 0.25

try:
    from openaq.shared.exceptions import RateLimitError, HTTPRateLimitError
except Exception:  # pragma: no cover - defensive fallback for SDK layout changes
    RateLimitError = type("RateLimitError", (Exception,), {})
    HTTPRateLimitError = type("HTTPRateLimitError", (Exception,), {})


class SharedRateAwareOpenAQGateway:
    """
    Thread-safe OpenAQ request gateway.

    Worker threads remain concurrent for orchestration, but all API calls pass
    through one shared client and one rate-aware lock-protected control path.
    """

    def __init__(
        self,
        api_key: str,
        measurements_limit: int = MEASUREMENTS_LIMIT,
        max_retries: int = MAX_SENSOR_RETRIES,
        fallback_rate_limit_wait_seconds: float = FALLBACK_RATE_LIMIT_WAIT_SECONDS,
        reset_jitter_seconds: float = RESET_JITTER_SECONDS,
    ):
        self.client = OpenAQ(api_key=api_key)
        self.measurements_limit = measurements_limit
        self.max_retries = max_retries
        self.fallback_rate_limit_wait_seconds = fallback_rate_limit_wait_seconds
        self.reset_jitter_seconds = reset_jitter_seconds

        self._lock = threading.Lock()
        self._next_allowed_monotonic = 0.0

        self.requests_attempted = 0
        self.retries = 0
        self.rate_limit_errors = 0
        self.http_rate_limit_errors = 0
        self.total_wait_seconds = 0.0

    def close(self) -> None:
        """Close underlying SDK resources."""
        try:
            self.client.close()
        except Exception:
            pass

    def _wait_if_needed_locked(self) -> None:
        now = time.monotonic()
        if now >= self._next_allowed_monotonic:
            return
        wait_seconds = self._next_allowed_monotonic - now
        self.total_wait_seconds += wait_seconds
        time.sleep(wait_seconds)

    def _schedule_next_allowed_locked(self, wait_seconds: float, with_jitter: bool = True) -> None:
        jitter_seconds = self.reset_jitter_seconds if with_jitter else 0.0
        target = time.monotonic() + max(0.0, wait_seconds) + jitter_seconds
        if target > self._next_allowed_monotonic:
            self._next_allowed_monotonic = target

    def _read_rate_limit_reset_seconds_locked(self) -> float:
        reset_seconds = getattr(self.client, "_rate_limit_reset_seconds", None)
        if reset_seconds is None:
            return self.fallback_rate_limit_wait_seconds
        try:
            reset_value = float(reset_seconds)
        except Exception:
            return self.fallback_rate_limit_wait_seconds
        if reset_value < 0:
            return 0.0
        return reset_value

    def _apply_post_success_rate_state_locked(self) -> None:
        """
        Pace future calls from SDK-reported rate state.

        The SDK updates `_rate_limit_remaining` and reset timing after each
        response. To keep throughput high, only schedule a wait when remaining
        budget is exhausted.
        """
        remaining = getattr(self.client, "_rate_limit_remaining", None)
        if remaining is None:
            return
        try:
            remaining_value = float(remaining)
        except Exception:
            return

        if remaining_value <= 0:
            reset_seconds = self._read_rate_limit_reset_seconds_locked()
            self._schedule_next_allowed_locked(reset_seconds, with_jitter=True)

    def fetch_measurements(self, sensor_id: str, from_date: str, to_date: str):
        """Rate-aware request for one sensor with retry handling."""
        attempt = 0
        while True:
            with self._lock:
                self._wait_if_needed_locked()
                self.requests_attempted += 1
                try:
                    response = self.client.measurements.list(
                        sensors_id=sensor_id,
                        datetime_from=from_date,
                        datetime_to=to_date,
                        limit=self.measurements_limit,
                    )
                    self._apply_post_success_rate_state_locked()
                    return response
                except RateLimitError:
                    self.rate_limit_errors += 1
                    if attempt >= self.max_retries:
                        raise
                    attempt += 1
                    self.retries += 1
                    self._schedule_next_allowed_locked(
                        self._read_rate_limit_reset_seconds_locked(),
                        with_jitter=True,
                    )
                    continue
                except HTTPRateLimitError:
                    self.http_rate_limit_errors += 1
                    if attempt >= self.max_retries:
                        raise
                    attempt += 1
                    self.retries += 1
                    self._schedule_next_allowed_locked(
                        self._read_rate_limit_reset_seconds_locked(),
                        with_jitter=True,
                    )
                    continue
                except Exception:
                    # Non-rate-limit transient error retry path with short backoff.
                    if attempt >= self.max_retries:
                        raise
                    attempt += 1
                    self.retries += 1
                    backoff_seconds = min(8.0, 2.0 ** attempt)
                    self._schedule_next_allowed_locked(backoff_seconds, with_jitter=False)
                    continue

def get_single_sensor_data(sensor_id: str, from_date: str, to_date: str, gateway: SharedRateAwareOpenAQGateway):
    """
    Get data for a single sensor.
    
    Args:
        sensor_id: Sensor ID to query
        from_date: Start date for data collection
        to_date: End date for data collection
        gateway: Shared rate-aware OpenAQ request gateway
        
    Returns:
        DataFrame with sensor data, or empty DataFrame if no data/error
    """
    try:
        response = gateway.fetch_measurements(sensor_id, from_date, to_date)
        
        # Process response
        data_measurements = response.dict()
        df_hourly_data = json_normalize(data_measurements['results'])
        
        if len(df_hourly_data) == 0:
            return pd.DataFrame(), "no_data"
            
        # Rename columns 
        df_hourly_data = df_hourly_data.rename(columns={
            'period.datetime_from.local': 'datetime_from_local',
            'period.datetime_from.utc': 'datetime_from_utc',
            'period.datetime_to.local': 'datetime_to_local',
            'period.datetime_to.utc': 'datetime_to_utc',
            'parameter.name': 'sensor_type',
            'parameter.units': 'sensor_units'
        })

        # Re-integrate sensor id
        df_hourly_data['sensor_id'] = sensor_id
        
        # Keep only relevant columns
        df_hourly_data = df_hourly_data[["value", "datetime_from_utc", "datetime_from_local", 
                                       "datetime_to_utc", "datetime_to_local", "sensor_type", 
                                       "sensor_units", "sensor_id"]]
        
        return df_hourly_data, "success"
        
    except Exception as e:
        print(f"{sensor_id} - failed: {str(e)}")
        return pd.DataFrame(), "error"

def process_sensor_batch(sensor_batch, from_date, to_date, gateway: SharedRateAwareOpenAQGateway):
    """
    Process a batch of sensors concurrently.
    
    Args:
        sensor_batch: List of sensor IDs to process
        from_date: Start date for data collection
        to_date: End date for data collection
        gateway: Shared rate-aware OpenAQ request gateway
        
    Returns:
        List of DataFrames with results, and statistics dict
    """
    results = []
    stats = {"success": 0, "no_data": 0, "errors": 0}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_sensor = {
            executor.submit(get_single_sensor_data, sensor_id, from_date, to_date, gateway): sensor_id 
            for sensor_id in sensor_batch
        }
        
        # Collect results
        for future in as_completed(future_to_sensor):
            sensor_id = future_to_sensor[future]
            try:
                df, status = future.result()
                if not df.empty:
                    results.append(df)
                    stats["success"] += 1
                elif status == "no_data":
                    stats["no_data"] += 1
                else:
                    stats["errors"] += 1
            except Exception as e:
                print(f"Error processing sensor {sensor_id}: {str(e)}")
                stats["errors"] += 1
    
    return results, stats

def get_sensors_data(sensor_list, from_date, to_date, gateway: SharedRateAwareOpenAQGateway, gdf_sensors):
    """
    Get measurements data for a list of sensors using parallel processing.
    
    Args:
        sensor_list: List of sensor IDs
        from_date: Start date for data collection
        to_date: End date for data collection
        gateway: Shared rate-aware OpenAQ request gateway
        gdf_sensors: DataFrame with sensor metadata
        
    Returns:
        DataFrame with sensor measurements
    """
    print(f"From date: {from_date}")
    print(f"To date: {to_date}")
    print(f"Number of sensors to extract: {len(sensor_list)}")
    print(f"Processing in batches of {BATCH_SIZE} with {MAX_WORKERS} workers per batch")

    all_data = []
    total_stats = {"success": 0, "no_data": 0, "errors": 0}
    
    # Process sensors in batches
    total_batches = (len(sensor_list) + BATCH_SIZE - 1) // BATCH_SIZE
    
    with tqdm(total=len(sensor_list), desc="Processing sensors", 
              unit="sensor", ncols=100) as pbar:
        
        for i in range(0, len(sensor_list), BATCH_SIZE):
            batch = sensor_list[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            
            pbar.set_description(f"Batch {batch_num}/{total_batches}")
            
            # Process batch in parallel
            batch_results, batch_stats = process_sensor_batch(batch, from_date, to_date, gateway)
            
            # Collect results
            all_data.extend(batch_results)
            
            # Update total statistics
            for key in total_stats:
                total_stats[key] += batch_stats[key]
            
            # Update progress bar
            pbar.update(len(batch))
            pbar.set_postfix(
                success=total_stats["success"], 
                no_data=total_stats["no_data"], 
                errors=total_stats["errors"]
            )
            
    # Display final statistics
    print(f"\nCollection completed: {total_stats['success']} successful, "
          f"{total_stats['no_data']} no data, {total_stats['errors']} errors")
    print(
        "Rate-aware gateway stats: "
        f"requests_attempted={gateway.requests_attempted}, "
        f"retries={gateway.retries}, "
        f"rate_limit_errors={gateway.rate_limit_errors}, "
        f"http_rate_limit_errors={gateway.http_rate_limit_errors}, "
        f"wait_seconds={gateway.total_wait_seconds:.2f}"
    )

    # Combine all data
    if not all_data:
        print("No data collected from any sensor")
        return pd.DataFrame()
    
    print(f"Combining data from {len(all_data)} successful sensors...")
    df_concat_hourly_data = pd.concat(all_data, ignore_index=True)

    # Merge with sensor metadata (same as before)
    if not df_concat_hourly_data.empty and not gdf_sensors.empty:
        try:
            # Ensure sensor_id is string in both dataframes for proper merging
            gdf_sensors['sensor_id'] = gdf_sensors['sensor_id'].astype(str)
            df_concat_hourly_data['sensor_id'] = df_concat_hourly_data['sensor_id'].astype(str)
            
            # Merge with sensor metadata
            desired_columns = ['location_id', 'name', 'sensor_id', 'country', 'latitude', 
                             'longitude', 'owner_name', 'provider_name']
            # Add sensor_grade if it exists
            if 'sensor_grade' in gdf_sensors.columns:
                desired_columns.append('sensor_grade')
                
            # Only keep columns that exist in gdf_sensors
            existing_columns = [col for col in desired_columns if col in gdf_sensors.columns]
            
            df_concat_hourly_data = df_concat_hourly_data.merge(
                gdf_sensors[existing_columns], 
                on='sensor_id',
                how='left'
            )
            
            # Keep relevant columns (ensuring they exist)
            all_desired_columns = ['location_id', 'sensor_id', 'datetime_from_local', 'datetime_from_utc',
                                 'latitude', 'longitude', 'sensor_type', 'sensor_units', 'value',
                                 'provider_name', 'owner_name', 'country', 'name']
            if 'sensor_grade' in df_concat_hourly_data.columns:
                all_desired_columns.append('sensor_grade')
                
            # Only keep columns that exist
            existing_cols = [col for col in all_desired_columns if col in df_concat_hourly_data.columns]
            df_concat_hourly_data = df_concat_hourly_data[existing_cols]
            
        except Exception as e:
            print(f"Error merging data: {str(e)}")
            # Return unmerged data if there's an error
            pass

    return df_concat_hourly_data

def main():
    parser = argparse.ArgumentParser(description="Collect real-time data from OpenAQ")
    parser.add_argument("--days", type=int, default=2,
                        help="Number of days to look back for data (default: 2)")
    parser.add_argument("--locations", type=int, nargs="+", 
                        help="Specific location IDs to query (optional)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit the number of locations to process (default: no limit - process all)")
    parser.add_argument("--output", type=str, default="data/raw/openaq/realtime",
                        help="Directory to save output (default: data/raw/openaq/realtime)")
    parser.add_argument("--test-mode", action="store_true",
                        help="Enable bounded test mode for fast deterministic runs")
    parser.add_argument("--test-limit", type=int, default=int(os.getenv("AIR_QUALITY_TEST_OPENAQ_LIMIT", "5")),
                        help="Maximum locations processed in test mode (default: AIR_QUALITY_TEST_OPENAQ_LIMIT or 5)")
    args = parser.parse_args()

    test_mode = args.test_mode or os.getenv("AIR_QUALITY_TEST_MODE") == "1"
    if test_mode:
        global MAX_WORKERS, BATCH_SIZE
        MAX_WORKERS = 1
        BATCH_SIZE = 5
        args.days = min(args.days, 1)
        if args.limit is None:
            args.limit = args.test_limit
        else:
            args.limit = min(args.limit, args.test_limit)
        print(
            f"Running OpenAQ realtime client in test mode "
            f"(days={args.days}, location_limit={args.limit}, workers={MAX_WORKERS}, batch_size={BATCH_SIZE})"
        )
    
    # Load environment variables from .env file if it exists
    env_path = Path('.env')
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    
    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize OpenAQ collector to get API key and sensors
    collector = OpenAQCollector()
    api_key = collector.get_api_key_from_config()
    
    # Determine date range
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=args.days)
    
    # Get sensor data through the collector
    print("\nFetching sensor data...")
    gdf_sensors = collector._get_sensors_country()
    
    if gdf_sensors.empty:
        print("Error: No sensor data found")
        return
    
    # Filter sensors by location if specified
    if args.locations:
        print(f"\nFiltering for {len(args.locations)} specified locations: {args.locations}")
        gdf_sensors = gdf_sensors[gdf_sensors['location_id'].isin(args.locations)]
        if gdf_sensors.empty:
            print("No sensors found for the specified locations.")
            return
    
    # Get unique locations and limit if needed
    locations = gdf_sensors['location_id'].unique()
    if args.limit and len(locations) > args.limit and not args.locations:
        print(f"\nLimiting to {args.limit} locations out of {len(locations)} total")
        locations = locations[:args.limit]
        gdf_sensors = gdf_sensors[gdf_sensors['location_id'].isin(locations)]
    else:
        print(f"\nProcessing all {len(locations)} locations (no limit applied)")
    
    # Get unique sensor IDs
    sensor_list = gdf_sensors['sensor_id'].astype(str).unique().tolist()
    print(f"Found {len(sensor_list)} unique sensors from {len(locations)} locations")
    
    # Start timer
    start_time = time.time()
    
    # Collect sensor data
    print("\nCollecting real-time data...")
    gateway = SharedRateAwareOpenAQGateway(
        api_key=api_key,
        measurements_limit=MEASUREMENTS_LIMIT,
    )
    try:
        df_data = get_sensors_data(
            sensor_list,
            from_date.strftime("%Y-%m-%d"),
            to_date.strftime("%Y-%m-%d"),
            gateway,
            gdf_sensors
        )
    finally:
        gateway.close()
    
    # Calculate time taken
    elapsed_time = time.time() - start_time
    print(f"\nData collection completed in {elapsed_time:.2f} seconds")
    
    if not df_data.empty:
        # Print summary
        print(f"\nCollected {len(df_data)} data points")
        print(f"Data for {df_data['sensor_id'].nunique()} unique sensors")
        print(f"Data for {df_data['location_id'].nunique()} unique locations")
        
        # Group by location for stats
        if 'location_id' in df_data.columns:
            location_counts = df_data.groupby('location_id').size()
            print("\nRecord counts by location:")
            for loc_id, count in location_counts.items():
                name = df_data[df_data['location_id'] == loc_id]['name'].iloc[0] if 'name' in df_data.columns else "Unknown"
                print(f"  - Location {loc_id} ({name}): {count} records")
        
        # Show sample data
        print("\nSample data:")
        print(df_data.head())
        
        # Save the data
        # Use country codes for filename instead of timestamp
        country_codes = collector.config['data_collection'].get('country_codes_openaq', 
                                                                 collector.config['data_collection']['country_codes'])
        
        # Map numeric OpenAQ country codes to ISO country codes
        code_mapping = {
            68: "LAO",   # Laos
            111: "THA"   # Thailand
        }
        
        # Convert numeric codes to ISO codes
        iso_codes = []
        for code in sorted(country_codes):
            if code in code_mapping:
                iso_codes.append(code_mapping[code])
            else:
                # Fallback to original code if mapping not found
                iso_codes.append(str(code))
        
        countries_str = '_'.join(iso_codes)
        from_date_str = from_date.strftime("%Y-%m-%d")
        to_date_str = to_date.strftime("%Y-%m-%d")
        parquet_file = output_dir / f"openaq_realtime_{countries_str}_from_{from_date_str}_to_{to_date_str}.parquet"
        
        print(f"\nSaving data to {parquet_file}")
        df_data.to_parquet(parquet_file, index=False)
        print("Data saved successfully!")
    else:
        print("No data collected!")

if __name__ == "__main__":
    main() 
