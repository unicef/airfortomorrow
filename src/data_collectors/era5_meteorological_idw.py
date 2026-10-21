#!/usr/bin/env python3
"""
ERA5 Meteorological Data Collector with IDW Interpolation

This module collects ERA5 meteorological data from ECMWF Open Data API
and processes it into H3 hexagonal grid format using Inverse Distance Weighting (IDW)
interpolation for air quality prediction.

Supports both real-time and historical data collection modes.
"""

import argparse
import logging
import os
import random
import re
import sys
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import warnings
from dotenv import load_dotenv
import time
from functools import reduce
import io
import threading
import requests

import earthkit.data as ekd
from earthkit.data import config

import polars as pl
import polars_h3 as plh3
import pandas as pd
import earthkit.data
import xarray as xr
import geopandas as gpd
from shapely.ops import unary_union
from h3ronpy.pandas.vector import geodataframe_to_cells

# Load environment variables from .env file
load_dotenv()

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)


def create_boundary_grid(countries: List[str] = ["LAO", "THA"], buffer_degrees: float = 0.4) -> pl.LazyFrame:
    """
    Create H3 boundary grid for specified countries.
    
    Args:
        countries: List of country codes
        buffer_degrees: Buffer around boundaries in degrees (not used with h3ronpy)
        
    Returns:
        Polars LazyFrame with H3 boundary cells
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Creating H3 boundary grid for countries: {countries}")
    
    country_codes = countries
    all_boundaries = []

    for country_code in country_codes:
        try:
            logger.info(f"Processing {country_code}...")
            
            # Use requests.get() approach (works in Docker) instead of gpd.read_file() directly
            url = f"https://github.com/wmgeolab/geoBoundaries/raw/fcccfab7523d4d5e55dfc7f63c166df918119fd1/releaseData/gbOpen/{country_code}/ADM0/geoBoundaries-{country_code}-ADM0.geojson"
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            resp.raise_for_status()
            boundary = gpd.read_file(io.BytesIO(resp.content))
            
            # Skip buffer operation - h3ronpy can't handle buffered geometries properly
            logger.info(f"Loaded boundary for {country_code} (no buffer applied for h3ronpy compatibility)")
            
            # Convert to H3 cells
            boundary_gdf = geodataframe_to_cells(
                boundary,
                8,  # H3 resolution 8
            )
            
            # Convert to Polars
            boundary = pl.DataFrame(boundary_gdf).select("cell")
            boundary = boundary.lazy()
            all_boundaries.append(boundary)
            
            logger.info(f"Added {len(boundary_gdf)} H3 cells for {country_code}")
            
        except Exception as e:
            logger.error(f"Error processing country {country_code}: {e}")
            continue

    if not all_boundaries:
        raise ValueError("No valid country boundaries found")
    
    # Concatenate and remove duplicates
    final_boundary = pl.concat(all_boundaries).unique(subset=["cell"])
    
    # Rename to match our convention
    final_boundary = final_boundary.rename({"cell": "h3_08"})
    
    total_cells = final_boundary.select(pl.len()).collect().item()
    logger.info(f"Created boundary grid with {total_cells} unique H3 cells (no buffer)")
    
    return final_boundary


def inverse_distance_weighting(
    ds: pl.LazyFrame,
    boundary: pl.LazyFrame,
    RINGS: int = 10,
    weight_power: float = 1.5,
    h3_column: str = "h3_08",
) -> pl.DataFrame:
    """
    Perform Inverse Distance Weighting interpolation to fill missing H3 cells.
    
    Args:
        ds: LazyFrame with known values
        boundary: LazyFrame with boundary H3 cells
        RINGS: Number of rings for grid disk search
        weight_power: Power for distance weighting (default 1.5)
        h3_column: Name of H3 column
        
    Returns:
        DataFrame with interpolated values for all boundary cells
    """
    logger = logging.getLogger(__name__)
    
    # Variables and expressions for weighting / aggregation
    exclusion_cols = ["latitude", "longitude", h3_column, "time", "valid_time"]
    value_cols = [c for c in ds.collect_schema().names() if c not in exclusion_cols]

    logger.info(f"Value columns for IDW: {value_cols}")
    weight_expr = 1.0 / (pl.col("dist").cast(pl.Float32) ** weight_power)
    wv_exprs = [
        (pl.col(value) * weight_expr).alias(f"wv_{value}") for value in value_cols
    ]
    agg_exprs = [pl.sum("w").alias("wsum")] + [
        pl.sum(f"wv_{value}").alias(f"sum_{value}") for value in value_cols
    ]

    # Use select(pl.len()) instead of len(collect()) to avoid loading data into memory
    logger.info(f"Dataset length: {ds.select(pl.len()).collect().item()}")
    logger.info(f"Boundary length: {boundary.select(pl.len()).collect().item()}")

    # Ensure both h3_08 columns have the same data type (cast to u64)
    ds_fixed = ds.with_columns(pl.col(h3_column).cast(pl.UInt64).alias(h3_column))
    boundary_fixed = boundary.with_columns(pl.col(h3_column).cast(pl.UInt64).alias(h3_column))

    missing_hexes = (
        boundary_fixed.select(pl.col(h3_column))
        .unique()
        .join(ds_fixed.select(h3_column), on=h3_column, how="anti")
        .select(h3_column)
        .lazy()
    )

    # Use select(pl.len()) instead of len(collect()) to avoid full data load
    logger.info(f"Missing hexes: {missing_hexes.select(pl.len()).collect().item()}")

    # Debug: Check memory usage before the heavy operation
    import psutil
    process = psutil.Process()
    logger.info(f"Memory usage before grid_disk: {process.memory_info().rss / 1024 / 1024:.1f} MB")

    # Use sink_parquet to avoid memory explosion
    tmp = "targets_tmp_era.parquet"

    logger.info("Starting grid_disk operation...")
    
    # Use sink_parquet directly without collecting to avoid memory explosion
    (
        ds_fixed.select([h3_column] + value_cols)
        .with_columns(plh3.grid_disk(h3_column, RINGS).alias("targets"))
        .explode("targets")
    ).sink_parquet(tmp)
    
    logger.info(f"Memory usage after sink_parquet: {process.memory_info().rss / 1024 / 1024:.1f} MB")

    contrib = (
        pl.scan_parquet(tmp)
        .join(missing_hexes, left_on="targets", right_on=h3_column, how="inner")
        .collect(engine="streaming")
    )

    contrib = contrib.with_columns(
        plh3.grid_distance(pl.col(h3_column), pl.col("targets")).alias("dist")
    )

    contrib = contrib.with_columns(
        [
            weight_expr.alias("w"),
            *[(pl.col(v) * weight_expr).alias(f"wv_{v}") for v in value_cols],
        ]
    )
    contrib = contrib.with_columns(pl.col("targets").alias(h3_column))
    contrib = contrib.select(h3_column, "w", *[f"wv_{v}" for v in value_cols])

    predicted = (
        contrib.group_by(h3_column)
        .agg(agg_exprs)
        .with_columns(
            [
                (pl.col(f"sum_{value}") / pl.col("wsum")).alias(
                    value
                )  # final prediction per var
                for value in value_cols
            ]
        )
        .select(h3_column, *value_cols)
    )

    known_vars = ds_fixed.select(h3_column, *value_cols)
    filled = (
        pl.concat([known_vars, predicted.lazy()])
        .group_by(h3_column, maintain_order=True)
        .agg(
            [pl.col(value).max().alias(value) for value in value_cols]
        )  # keep the original value where present
    )

    # Clean up temporary file
    if os.path.exists(tmp):
        os.remove(tmp)
    
    # Explicit cleanup to release memory
    del ds_fixed, boundary_fixed, missing_hexes, contrib, predicted, known_vars
    
    # Force garbage collection to ensure memory is released
    import gc
    gc.collect()

    return filled


class ERA5MeteorologicalCollectorIDW:
    """
    Collects and processes ERA5 meteorological data using IDW interpolation.
    
    Parameters:
    - 2t: 2-meter temperature
    - 10u: 10-meter u-component of wind  
    - 10v: 10-meter v-component of wind
    - 2d: 2-meter dewpoint temperature
    """
    
    # Default parameters and configuration
    DEFAULT_PARAMS = ["2d", "2t", "10u", "10v"]
    DEFAULT_STEPS = [0, 6, 12, 18, 24]  # Forecast steps in hours
    DEFAULT_H3_RESOLUTION = 8
    
    def __init__(self, 
                 output_dir: str = "./data/processed/era5/daily_aggregated",
                 raw_data_dir: str = "./data/raw/era5",
                 params: List[str] = None,
                 steps: List[int] = None,
                 countries: List[str] = None,
                 idw_rings: int = 10,
                 idw_weight_power: float = 1.5,
                 force_reprocess: bool = False):
        """
        Initialize ERA5 collector with IDW interpolation.
        
        Args:
            output_dir: Directory for processed daily aggregated data
            raw_data_dir: Directory for raw data (metadata)
            params: List of ERA5 parameters to collect
            steps: List of forecast steps (hours)
            countries: List of country codes
            idw_rings: Number of rings for IDW interpolation
            idw_weight_power: Power for distance weighting in IDW
            force_reprocess: If True, reprocess files even if they already exist
        """
        self.output_dir = Path(output_dir)
        self.raw_data_dir = Path(raw_data_dir)
        self.params = params or self.DEFAULT_PARAMS.copy()
        self.steps = steps or self.DEFAULT_STEPS.copy()
        self.countries = countries or ["THA", "LAO"]
        self.idw_rings = idw_rings
        self.idw_weight_power = idw_weight_power
        self.force_reprocess = force_reprocess
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Create directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

        # Set up cache
        cache_dir = Path(self.raw_data_dir) / "earthkit_cache"
        ekd.config.set({
            "cache-policy": "user",
            "user-cache-directory": str(cache_dir),
            "number-of-download-threads": 1,
            "url-download-timeout": "120s",
            "check-out-of-date-urls": False
        })

        self.logger.info(
        "earthkit: cache=%s dir=%s threads=%s timeout=%s",
        config.get("cache-policy"),
        config.get("user-cache-directory"),
        config.get("number-of-download-threads"),
        config.get("url-download-timeout"),
        )
        
        self.logger.info(f"ERA5 Collector with IDW initialized")
        self.logger.info(f"Countries: {self.countries}")
        self.logger.info(f"Parameters: {self.params}")
        self.logger.info(f"Steps: {self.steps}")
        self.logger.info(f"IDW rings: {self.idw_rings}")
        self.logger.info(f"IDW weight power: {self.idw_weight_power}")
        self.logger.info(f"Output directory: {self.output_dir}")

        # Initialize rate limiting
        self._last_request_time = 0
        self._rate_limit_lock = threading.Lock()
        self._rate_limit_config = self._get_rate_limiting_config()
        self.logger.info(f"Rate limiting configured: {self._rate_limit_config['requests_per_minute']} requests/minute")
    
    def _get_rate_limiting_config(self) -> dict:
        """Get rate limiting configuration from config file or use defaults."""
        try:
            from src.utils.config_loader import ConfigLoader
            config_loader = ConfigLoader()
            era5_config = config_loader.get_data_collection_config('era5')
            rate_limiting = era5_config.get('rate_limiting', {})
            
            return {
                'requests_per_minute': rate_limiting.get('requests_per_minute', 10),
                'delay_between_requests': rate_limiting.get('delay_between_requests', 6.0),
                'max_retries': rate_limiting.get('max_retries', 5),
                'backoff_factor': rate_limiting.get('backoff_factor', 2.0),
                'retry_delay_base': rate_limiting.get('retry_delay_base', 60)
            }
        except Exception as e:
            self.logger.warning(f"Could not load rate limiting config, using defaults: {e}")
            return {
                'requests_per_minute': 10,
                'delay_between_requests': 6.0,
                'max_retries': 5,
                'backoff_factor': 2.0,
                'retry_delay_base': 60
            }
    
    def _rate_limit_wait(self):
        """Apply rate limiting delay between requests."""
        with self._rate_limit_lock:
            if hasattr(self, '_last_request_time') and self._last_request_time > 0:
                time_since_last = time.time() - self._last_request_time
                delay_needed = self._rate_limit_config['delay_between_requests']
                
                if time_since_last < delay_needed:
                    sleep_time = delay_needed - time_since_last
                    self.logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
                    time.sleep(sleep_time)
            
            self._last_request_time = time.time()

    @staticmethod
    def _extract_status_code(error: Exception) -> Optional[int]:
        """Best-effort extraction of HTTP status code from exception objects/messages."""
        response = getattr(error, "response", None)
        status_code = getattr(response, "status_code", None)
        if isinstance(status_code, int):
            return status_code

        for candidate in re.findall(r"\b([1-5]\d{2})\b", str(error)):
            parsed = int(candidate)
            if 100 <= parsed <= 599:
                return parsed
        return None

    @staticmethod
    def _extract_retry_after_seconds(error: Exception) -> Optional[float]:
        """Read Retry-After/X-RateLimit-Reset headers when available."""
        response = getattr(error, "response", None)
        headers = getattr(response, "headers", None)
        if not headers:
            return None

        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        if retry_after:
            retry_after = str(retry_after).strip()
            try:
                return max(0.5, float(retry_after))
            except ValueError:
                try:
                    reset_dt = parsedate_to_datetime(retry_after)
                    wait_seconds = reset_dt.timestamp() - time.time()
                    return max(0.5, wait_seconds)
                except Exception:
                    return None

        reset_header = headers.get("X-RateLimit-Reset") or headers.get("x-ratelimit-reset")
        if reset_header:
            try:
                reset_epoch = float(str(reset_header).strip())
                wait_seconds = reset_epoch - time.time()
                return max(0.5, wait_seconds)
            except Exception:
                return None

        return None

    def _classify_request_error(self, error: Exception) -> Dict[str, object]:
        """Classify request errors to decide retry behavior."""
        status_code = self._extract_status_code(error)
        error_text = str(error).lower()

        rate_limited = (
            status_code == 429
            or "too many requests" in error_text
            or "rate limit" in error_text
        )
        not_found = (
            status_code == 404
            or "not found" in error_text
            or "no such key" in error_text
        )

        retryable_status_codes = {408, 425, 429, 500, 502, 503, 504}
        retryable_transport_markers = (
            "timed out",
            "timeout",
            "temporarily unavailable",
            "connection reset",
            "connection aborted",
            "network",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
        )
        retryable_transport = any(
            marker in error_text for marker in retryable_transport_markers
        )

        retryable = (
            not not_found
            and (
                rate_limited
                or (status_code in retryable_status_codes if status_code is not None else False)
                or retryable_transport
            )
        )

        return {
            "status_code": status_code,
            "rate_limited": rate_limited,
            "not_found": not_found,
            "retryable": retryable,
        }

    def _compute_backoff_wait_seconds(
        self,
        *,
        attempt: int,
        error: Exception,
        classification: Dict[str, object],
        base_delay_seconds: float,
        backoff_factor: float,
        max_delay_seconds: float,
    ) -> float:
        """Compute adaptive retry wait with header-aware 429 handling and jitter."""
        if classification.get("rate_limited"):
            retry_after = self._extract_retry_after_seconds(error)
            if retry_after is not None:
                return min(max_delay_seconds, retry_after)

        exponential_wait = base_delay_seconds * (backoff_factor ** attempt)
        jittered_wait = exponential_wait * random.uniform(0.8, 1.2)
        return min(max_delay_seconds, max(0.5, jittered_wait))

    def _execute_request_with_backoff(
        self,
        request_func,
        *,
        request_label: str,
        max_retries: int,
        base_delay_seconds: float,
        backoff_factor: float,
        max_delay_seconds: float,
        apply_rate_limit_wait: bool = False,
    ) -> Tuple[bool, Optional[object], str, Optional[Exception]]:
        """
        Execute request with bounded retries.

        Returns:
            Tuple of (success, result, failure_reason, last_error)
        """
        for attempt in range(max_retries + 1):
            try:
                if apply_rate_limit_wait:
                    self._rate_limit_wait()
                result = request_func()
                return True, result, "success", None
            except Exception as error:
                classification = self._classify_request_error(error)
                status_code = classification.get("status_code")

                if classification.get("not_found"):
                    self.logger.info(
                        f"{request_label}: data not available yet (404/not found), skipping."
                    )
                    return False, None, "not_found", error

                if not classification.get("retryable"):
                    self.logger.error(
                        f"{request_label}: non-retryable error"
                        f"{f' (status={status_code})' if status_code else ''}: {error}"
                    )
                    return False, None, "non_retryable", error

                if attempt >= max_retries:
                    self.logger.error(
                        f"{request_label}: max retries exceeded after {max_retries + 1} attempts"
                        f"{f' (status={status_code})' if status_code else ''}: {error}"
                    )
                    return False, None, "max_retries_exceeded", error

                wait_seconds = self._compute_backoff_wait_seconds(
                    attempt=attempt,
                    error=error,
                    classification=classification,
                    base_delay_seconds=base_delay_seconds,
                    backoff_factor=backoff_factor,
                    max_delay_seconds=max_delay_seconds,
                )
                attempt_label = f"{attempt + 1}/{max_retries + 1}"
                reason = "429/rate-limit" if classification.get("rate_limited") else "transient error"
                self.logger.warning(
                    f"{request_label}: {reason}; retrying ({attempt_label}) in {wait_seconds:.2f}s"
                    f"{f' [status={status_code}]' if status_code else ''}"
                )
                time.sleep(wait_seconds)

        return False, None, "unknown", None
    
    def _make_rate_limited_request(self, request_func, *args, **kwargs):
        """Make a rate-limited request with retry logic."""
        max_retries = self._rate_limit_config['max_retries']
        backoff_factor = self._rate_limit_config['backoff_factor']
        retry_delay_base = self._rate_limit_config['retry_delay_base']
        max_delay_seconds = max(float(retry_delay_base), 300.0)

        success, result, failure_reason, last_error = self._execute_request_with_backoff(
            lambda: request_func(*args, **kwargs),
            request_label="ERA5 API request",
            max_retries=max_retries,
            base_delay_seconds=float(retry_delay_base),
            backoff_factor=float(backoff_factor),
            max_delay_seconds=max_delay_seconds,
            apply_rate_limit_wait=True,
        )

        if success:
            return result

        if last_error is not None:
            raise last_error

        raise RuntimeError(f"ERA5 API request failed with reason: {failure_reason}")
    
    def collect_historical_data(self, start_date: str, end_date: str) -> List[str]:
        """
        Collect historical ERA5 reanalysis data for date range with IDW interpolation.
        Downloads and processes one date at a time to avoid memory issues.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of output file paths
        """
        self.logger.info(f"Collecting historical ERA5 reanalysis data from {start_date} to {end_date}")
        
        # Check for CDS API credentials - earthkit expects CDSAPI_KEY and CDSAPI_URL
        cds_key = os.getenv('CDSAPI_KEY')
        cds_url = os.getenv('CDSAPI_URL')
        
        self.logger.info(f"CDSAPI_KEY found: {cds_key is not None}")
        self.logger.info(f"CDSAPI_URL found: {cds_url is not None}")
        
        if cds_key and cds_url:
            # Environment variables are already in the correct format for earthkit
            self.logger.info("Using CDS credentials from environment variables (CDSAPI_KEY/CDSAPI_URL)")
        else:
            # Check for ~/.cdsapirc file
            cdsapirc_path = os.path.expanduser('~/.cdsapirc')
            if not os.path.exists(cdsapirc_path):
                error_msg = (
                    "CDS API credentials not found. Please either:\n"
                    "1. Set environment variables: CDSAPI_KEY and CDSAPI_URL\n"
                    "2. Create ~/.cdsapirc file with your credentials\n"
                    "Get your API key from: https://cds.climate.copernicus.eu/user"
                )
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            else:
                self.logger.info("Using CDS credentials from ~/.cdsapirc file")
        
        try:
            # Generate date range for reanalysis
            dates = pd.date_range(start_date, end_date).strftime("%Y-%m-%d").tolist()
            self.logger.info(f"Processing {len(dates)} dates sequentially (one API call per date)")
            self.logger.info(f"Date list: {dates}")
            
            # Convert parameter names for reanalysis API
            param_mapping = {
                "2t": "2m_temperature",
                "10u": "10m_u_component_of_wind", 
                "10v": "10m_v_component_of_wind",
                "2d": "2m_dewpoint_temperature"
            }
            
            variables = [param_mapping.get(p, p) for p in self.params]
            self.logger.info(f"Using reanalysis variables: {variables}")
            
            # Create boundary grid for all countries (once for all dates)
            boundary = create_boundary_grid(self.countries, buffer_degrees=0)
            
            # Calculate area bounds for all countries
            area = self._calculate_area_bounds()
            
            # Process each date individually to avoid memory issues
            output_paths = []
            import psutil
            process = psutil.Process()
            for idx, date_str in enumerate(dates, 1):
                try:
                    self.logger.info(f"Processing date {date_str} ({idx}/{len(dates)})...")
                    
                    # Check if file already exists (skip if present and not forcing reprocess)
                    output_path = self.output_dir / "historical" / f"era5_daily_mean_{date_str}_THA_LAO.parquet"
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    if output_path.exists() and not self.force_reprocess:
                        self.logger.info(f"File already exists, skipping: {output_path.name}")
                        output_paths.append(str(output_path))
                        continue
                    elif output_path.exists() and self.force_reprocess:
                        self.logger.info(f"File exists but force_reprocess=True, reprocessing: {output_path.name}")
                    
                    # Download data for this single date only
                    self.logger.info(f"Downloading ERA5 data for {date_str} from CDS...")
                    
                    def make_single_date_cds_request():
                        return earthkit.data.from_source(
                            "cds",
                            "reanalysis-era5-single-levels",
                            variable=variables,
                            product_type="reanalysis",
                            area=area,  # N,W,S,E
                            grid=[0.008, 0.008],  # High resolution grid
                            date=date_str,  # Single date only
                            time=["00:00", "06:00", "12:00", "18:00"],  # 4 times per day
                        )
                    
                    datasets = self._make_rate_limited_request(make_single_date_cds_request)
                    
                    self.logger.info(f"Retrieved {len(datasets)} files for {date_str}")
                    
                    # Process this date immediately
                    output_path_result = self._process_single_day_historical(datasets, date_str, boundary)
                    
                    if output_path_result:
                        output_paths.append(output_path_result)
                        self.logger.info(f"Successfully processed {date_str} ({idx}/{len(dates)})")
                    
                    # Explicit cleanup to release memory before next iteration
                    del datasets
                    
                    # Force garbage collection to ensure memory is released between dates
                    import gc
                    gc.collect()
                    self.logger.debug(f"Memory usage after GC: {process.memory_info().rss / 1024 / 1024:.1f} MB")
                    
                    # Add small delay between dates to avoid overwhelming CDS API
                    if idx < len(dates):
                        import time
                        delay = 2  # 2 seconds between date requests
                        self.logger.debug(f"Waiting {delay}s before next date request...")
                        time.sleep(delay)
                    
                except Exception as e:
                    self.logger.error(f"Error processing date {date_str}: {e}")
                    import traceback
                    self.logger.debug(traceback.format_exc())
                    continue
            
            self.logger.info(f"Historical data collection completed: {len(output_paths)}/{len(dates)} files generated")
            return output_paths
            
        except Exception as e:
            self.logger.error(f"Error collecting historical reanalysis data: {e}")
            raise
    
    def _calculate_area_bounds(self) -> List[float]:
        """Calculate area bounds for all countries combined."""
        # Use the same bounds as in the notebook
        lon_min, lon_max = 97.343396, 107.63509393600003
        lat_min, lat_max = 5.612850991000073, 22.509044950000032
        return [lat_max, lon_min, lat_min, lon_max]  # N,W,S,E
    
    def _process_single_day_historical(self, datasets, date_str: str, boundary: pl.LazyFrame) -> Optional[str]:
        """
        Process a single day of historical data with IDW interpolation.
        
        Args:
            datasets: Earthkit datasets object
            date_str: Date string in YYYY-MM-DD format
            boundary: Boundary grid LazyFrame
            
        Returns:
            Output file path or None if failed
        """
        try:
            # Create output path
            output_path = self.output_dir / "historical" / f"era5_daily_mean_{date_str}_THA_LAO.parquet"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Skip if file exists (unless forcing reprocess)
            if output_path.exists() and not self.force_reprocess:
                self.logger.debug(f"File already exists: {output_path}")
                return str(output_path)
            elif output_path.exists() and self.force_reprocess:
                self.logger.debug(f"File exists but force_reprocess=True, reprocessing: {output_path}")
            
            # Convert date string to datetime for filtering
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            date_int = int(date_obj.strftime("%Y%m%d"))
            
            # Get datasets for this date
            ds_list = pl.DataFrame(datasets.ls().reset_index())
            datasets_today = ds_list.filter(pl.col("dataDate") == date_int)
            ds_ids = datasets_today["index"].to_list()
            
            if not ds_ids:
                self.logger.warning(f"No data found for date {date_str}")
                return None
            
            # Convert to DataFrame
            ds = pl.DataFrame(datasets[ds_ids].to_xarray().to_dataframe().reset_index())
            
            # Add H3 indexing and drop unnecessary columns
            ds = ds.with_columns(h3_08=plh3.latlng_to_cell("latitude", "longitude", self.DEFAULT_H3_RESOLUTION))
            ds = ds.drop(["forecast_reference_time", "latitude", "longitude"])
            ds = ds.group_by("h3_08").mean().lazy()

            filtered_ds = (
                ds.lazy()
                .join(
                    boundary.lazy().select("h3_08").unique(),
                    on="h3_08",
                    how="semi"
                )
            ).lazy()
            
            # Apply IDW interpolation
            filled = inverse_distance_weighting(
                filtered_ds,
                boundary,
                self.idw_rings,
                self.idw_weight_power,
                "h3_08"
            )
            
           
            filtered_filled=filled.collect()
            
            # Add date column
            filtered_filled = filtered_filled.with_columns(pl.lit(date_obj.date()).alias("date"))
            
            # Save output
            filtered_filled.write_parquet(output_path, compression="zstd")
            # filtered_filled.sink_parquet(output_path, compression="zstd")
            self.logger.info(f"Saved: {output_path} with {len(filtered_filled)} cells")
            
            # Explicit cleanup to release memory
            del ds, filtered_ds, filled, filtered_filled, ds_list, datasets_today
            
            return str(output_path)
            
        except Exception as e:
            self.logger.error(f"Error processing date {date_str}: {e}")
            return None
    
    def collect_realtime_data(self, hours_back: int = 24) -> List[str]:
        """
        Collect real-time ERA5 data for the past N hours with IDW interpolation.
        Uses two-tier approach:
        - Day 0 (today): Live ECMWF Open Data API (latest forecast, always available)
        - Days 1-6 (past): ECMWF AWS mirror (historical forecasts with 1-3 day lag)
        
        Args:
            hours_back: Number of hours to look back
            
        Returns:
            List of output file paths
        """
        from ecmwf.opendata import Client
        
        days_back = max(1, hours_back // 24)
        self.logger.info(f"Collecting real-time data (past {hours_back} hours = {days_back} days)")
        
        try:
            # Calculate dates to collect
            today = datetime.now().date()
            dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") 
                     for i in range(days_back)]  # e.g., for 7 days: 0, 1, 2, 3, 4, 5, 6
            
            self.logger.info(f"Dates to collect: {dates}")
            self.logger.info(f"  - Day 0 ({dates[0]}): Live ECMWF Open Data API (latest forecast)")
            if len(dates) > 1:
                self.logger.info(f"  - Days 1-{len(dates)-1} ({dates[1]} to {dates[-1]}): AWS mirror (historical forecasts)")
            
            # Map parameters to ECMWF codes
            param_mapping = {
                "2t": "2t",
                "10u": "10u",
                "10v": "10v",
                "2d": "2d"
            }
            params = [param_mapping.get(p, p) for p in self.params]
            
            # Create boundary grid once (used by both live API and AWS mirror)
            boundary = create_boundary_grid(self.countries, buffer_degrees=0)
            
            output_files = []
            
            # STEP 1: Get today's forecast from Live API (idx=0, date=today)
            if len(dates) > 0:
                today_str = dates[0]
                self.logger.info(f"=" * 60)
                self.logger.info(f"STEP 1: Collecting TODAY's forecast ({today_str}) from Live API")
                self.logger.info(f"=" * 60)
                
                try:
                    # Get current forecast data from ECMWF Open Data with rate limiting
                    def make_ecmwf_request():
                        return earthkit.data.from_source(
                            "ecmwf-open-data",
                            param=params,
                            step=[0, 6, 12, 18, 24]  # Explicitly request all forecast steps
                        )
                    
                    self.logger.info(f"Requesting latest forecast from Live API...")
                    datasets = self._make_rate_limited_request(make_ecmwf_request)
                    self.logger.info(f"✅ Retrieved {len(datasets)} fields from Live API")
                    
                    # Process today's data
                    output_path = self._process_realtime_data(datasets, boundary, hours_back, date_str=today_str)
                    
                    if output_path:
                        output_files.append(output_path)
                        self.logger.info(f"✅ Successfully processed today's forecast: {output_path}")
                    else:
                        self.logger.warning(f"⚠️  Failed to process today's forecast")
                        
                except Exception as e:
                    self.logger.error(f"❌ Error getting today's forecast from Live API: {e}")
                    self.logger.warning("Continuing with past days from AWS mirror...")
            
            # STEP 2: Get past days (1-6) from AWS mirror
            if len(dates) > 1:
                past_dates = dates[1:]  # Skip today (index 0)
                self.logger.info(f"")
                self.logger.info(f"=" * 60)
                self.logger.info(f"STEP 2: Collecting past {len(past_dates)} days from AWS mirror")
                self.logger.info(f"=" * 60)
                self.logger.info(f"Past dates: {past_dates}")
                
                # Initialize AWS client
                client = Client(source="aws", model="ifs", resol="0p25")
                
                # Temp directory for GRIB files
                temp_dir = self.raw_data_dir / "aws_temp"
                temp_dir.mkdir(parents=True, exist_ok=True)
                
                # Collect data for each past date
                for idx, date_str in enumerate(past_dates):
                    try:
                        self.logger.info(f"Retrieving {date_str} from AWS mirror... ({idx+1}/{len(past_dates)})")

                        # Download GRIB file from AWS
                        grib_file = temp_dir / f"ifs-{date_str}.grib2"

                        # Retrieve data from AWS mirror with adaptive retry/backoff.
                        # No fixed inter-date sleep; we only wait when the server signals pressure/errors.
                        max_retries = 3
                        success, _, failure_reason, _ = self._execute_request_with_backoff(
                            lambda: client.retrieve(
                                date=date_str,
                                time=0,  # 00Z run
                                stream="oper",  # Operational
                                type="fc",  # Forecast
                                step=list(range(0, 25, 6)),  # 0, 6, 12, 18, 24 hours (similar to ERA5)
                                param=params,
                                target=str(grib_file),
                            ),
                            request_label=f"AWS retrieve {date_str}",
                            max_retries=max_retries,
                            base_delay_seconds=2.0,
                            backoff_factor=2.0,
                            max_delay_seconds=90.0,
                            apply_rate_limit_wait=False,
                        )

                        # Skip processing if data wasn't successfully retrieved
                        if not success:
                            if failure_reason != "not_found":
                                self.logger.warning(
                                    f"⚠️  Skipping {date_str} - data not available or download failed ({failure_reason})"
                                )
                            continue
                        
                        self.logger.info(f"✅ Downloaded GRIB file: {grib_file.name}")
                        
                        # Read GRIB file with earthkit
                        datasets = earthkit.data.from_source("file", str(grib_file))
                        self.logger.info(f"Loaded {len(datasets)} fields from GRIB file")
                        
                        # Process with existing logic
                        output_path = self._process_realtime_data(datasets, boundary, hours_back, date_str=date_str)
                        
                        if output_path:
                            output_files.append(output_path)
                            self.logger.info(f"✅ Processed {date_str}: {output_path}")
                        
                        # Clean up GRIB file
                        grib_file.unlink()
                        
                    except Exception as e:
                        self.logger.error(f"❌ Unexpected error processing {date_str}: {e}")
                        import traceback
                        self.logger.debug(traceback.format_exc())
                        continue
                
                # Clean up temp directory
                try:
                    temp_dir.rmdir()
                except:
                    pass
            
            # Final summary
            self.logger.info(f"")
            self.logger.info(f"=" * 60)
            self.logger.info(f"REALTIME COLLECTION SUMMARY")
            self.logger.info(f"=" * 60)
            self.logger.info(f"Total days requested: {days_back}")
            self.logger.info(f"Successfully collected: {len(output_files)} days")
            if len(output_files) < days_back:
                self.logger.warning(f"⚠️  Missing {days_back - len(output_files)} days (likely due to AWS mirror lag for recent dates)")
            else:
                self.logger.info(f"✅ Complete 7-day dataset available for predictions")
            self.logger.info(f"=" * 60)
            
            return output_files
            
        except Exception as e:
            self.logger.error(f"Error collecting real-time data: {e}")
            raise
    
    def _process_realtime_data(self, datasets, boundary: pl.LazyFrame, hours_back: int, date_str: str = None) -> Optional[str]:
        """
        Process real-time ERA5 data with IDW interpolation.
        
        Args:
            datasets: Earthkit datasets object from ECMWF Open Data
            boundary: Boundary grid LazyFrame
            hours_back: Number of hours to look back
            date_str: Optional date string (YYYY-MM-DD), defaults to today
            
        Returns:
            Output file path or None if failed
        """
        try:
            # Use provided date or default to today
            if date_str is None:
                date_str = datetime.now().strftime("%Y-%m-%d")
                target_date = datetime.now().date()
            else:
                target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            # Create output path for real-time data
            countries_str = "_".join(sorted(self.countries))
            output_path = self.output_dir / "realtime" / f"era5_daily_mean_{date_str}_{countries_str}.parquet"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Skip if file exists (unless forcing reprocess)
            if output_path.exists() and not self.force_reprocess:
                self.logger.debug(f"File already exists: {output_path}")
                return str(output_path)
            elif output_path.exists() and self.force_reprocess:
                self.logger.debug(f"File exists but force_reprocess=True, reprocessing: {output_path}")
            
            # Get metadata for the datasets
            df_metadata = datasets.ls()
            self.logger.info(f"Processing {len(df_metadata)} real-time files")
            
            # Convert to DataFrame
            ds = pl.DataFrame(datasets.to_xarray().to_dataframe().reset_index())
            
            # Debug: Check columns and data structure
            self.logger.info(f"Raw dataset columns: {ds.columns}")
            self.logger.info(f"Dataset shape: {ds.shape}")
            if "step" in ds.columns:
                step_unique = ds["step"].n_unique()
                self.logger.info(f"Number of unique steps: {step_unique}")
                self.logger.info(f"Step values: {ds['step'].unique().to_list()}")
            if "time" in ds.columns:
                time_unique = ds["time"].n_unique()
                self.logger.info(f"Number of unique times: {time_unique}")
            
            # Debug: Check the actual resolution of the data
            if len(ds) > 0:
                lat_diff = ds["latitude"].max() - ds["latitude"].min()
                lon_diff = ds["longitude"].max() - ds["longitude"].min()
                lat_unique = ds["latitude"].n_unique()
                lon_unique = ds["longitude"].n_unique()
                self.logger.info(f"Data bounds: lat({ds['latitude'].min():.3f}, {ds['latitude'].max():.3f}), lon({ds['longitude'].min():.3f}, {ds['longitude'].max():.3f})")
                self.logger.info(f"Data resolution: ~{lat_diff/lat_unique:.6f}° lat, ~{lon_diff/lon_unique:.6f}° lon")
                self.logger.info(f"Grid points: {lat_unique} × {lon_unique} = {lat_unique * lon_unique:,}")
            
            # Add H3 indexing and drop unnecessary columns
            ds = ds.with_columns(h3_08=plh3.latlng_to_cell("latitude", "longitude", self.DEFAULT_H3_RESOLUTION))
            ds = ds.drop(["latitude", "longitude"])
            
            # Group by h3_08 to aggregate spatially and temporally
            # This will average across all grid points in each H3 cell AND across all time steps
            self.logger.info("Grouping by h3_08 to create daily mean (aggregating across all time steps)")
            ds = ds.group_by("h3_08").mean()
            
            # Drop step column if it exists (it would have been averaged, but we don't need it)
            if "step" in ds.collect_schema().names():
                self.logger.info("Dropping step column after aggregation")
                ds = ds.drop("step")
            
            ds = ds.lazy()

                    # Filter to only include boundary cells
            filtered_ds = (
                ds.lazy()
                .join(
                    boundary.lazy().select("h3_08").unique(),
                    on="h3_08",
                    how="semi"
                )
               
            ).lazy()
            
            # Apply IDW interpolation
            filled = inverse_distance_weighting(
                filtered_ds,
                boundary,
                self.idw_rings,
                self.idw_weight_power,
                "h3_08"
            )
            
            filtered_filled=filled.collect()
            
            # Add date column with the specific target date
            filtered_filled = filtered_filled.with_columns(pl.lit(target_date).alias("date"))
            
            # Save output
            filtered_filled.write_parquet(output_path, compression="zstd")
            self.logger.info(f"Saved: {output_path} with {len(filtered_filled)} cells")
            
            return str(output_path)
            
        except Exception as e:
            self.logger.error(f"Error processing real-time data: {e}")
            return None


def setup_logging(log_dir: str = "./logs", log_level: str = "INFO"):
    """Setup logging configuration."""
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"era5_collector_idw_{timestamp}.log")
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return log_file


def main():
    """Main function for command-line execution."""
    parser = argparse.ArgumentParser(
        description='ERA5 Meteorological Data Collector with IDW Interpolation',
        epilog='''
CDS API Setup for Historical Mode:
Historical mode requires Climate Data Store (CDS) API credentials.
Set environment variables: CDSAPI_KEY="your-uid:your-api-key" and CDSAPI_URL="https://cds.climate.copernicus.eu/api"
Or create ~/.cdsapirc file. See docs/CDS_API_SETUP.md for details.
Get API key from: https://cds.climate.copernicus.eu/user
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Mode selection
    parser.add_argument('--mode', type=str, choices=['realtime', 'historical'], 
                       required=True, help='Collection mode')
    
    # Date parameters for historical mode
    parser.add_argument('--start-date', type=str,
                       help='Start date for historical mode (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                       help='End date for historical mode (YYYY-MM-DD)')
    
    # Real-time parameters
    parser.add_argument('--hours', type=int, default=24,
                       help='Hours to look back for real-time mode (default: 24)')
    
    # Data parameters
    parser.add_argument('--params', nargs='+', 
                       default=ERA5MeteorologicalCollectorIDW.DEFAULT_PARAMS,
                       help='ERA5 parameters to collect')
    parser.add_argument('--steps', nargs='+', type=int,
                       default=ERA5MeteorologicalCollectorIDW.DEFAULT_STEPS,
                       help='Forecast steps in hours')
    
    # Countries
    parser.add_argument('--countries', nargs='+', default=["THA", "LAO"],
                       help='Country codes for processing')
    
    # IDW parameters
    parser.add_argument('--idw-rings', type=int, default=10,
                       help='Number of rings for IDW interpolation')
    parser.add_argument('--idw-weight-power', type=float, default=1.5,
                       help='Power for distance weighting in IDW')
    
    # Directory parameters
    parser.add_argument('--output-dir', type=str, default="./data/processed/era5/daily_aggregated",
                       help='Output directory for processed data')
    parser.add_argument('--raw-data-dir', type=str, default="./data/raw/era5",
                       help='Directory for raw data metadata')
    
    # Logging
    parser.add_argument('--log-dir', type=str, default="./logs",
                       help='Log directory')
    parser.add_argument('--log-level', type=str, default="INFO",
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    # Processing options
    parser.add_argument('--force', action='store_true',
                       help='Force reprocessing even if output files already exist')
    
    args = parser.parse_args()
    
    # Setup logging
    log_file = setup_logging(args.log_dir, args.log_level)
    logger = logging.getLogger(__name__)
    
    logger.info("=== ERA5 Meteorological Data Collector with IDW Starting ===")
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Log file: {log_file}")
    
    # Validate arguments
    if args.mode == 'historical':
        if not args.start_date or not args.end_date:
            logger.error("Historical mode requires --start-date and --end-date")
            return 1
    
    try:
        # Initialize collector
        collector = ERA5MeteorologicalCollectorIDW(
            output_dir=args.output_dir,
            raw_data_dir=args.raw_data_dir,
            params=args.params,
            steps=args.steps,
            countries=args.countries,
            idw_rings=args.idw_rings,
            idw_weight_power=args.idw_weight_power,
            force_reprocess=args.force
        )
        
        # Collect data based on mode
        if args.mode == 'realtime':
            output_paths = collector.collect_realtime_data(args.hours)
        else:
            output_paths = collector.collect_historical_data(args.start_date, args.end_date)
        
        logger.info(f"=== Collection Complete ===")
        logger.info(f"Generated {len(output_paths)} files")
        logger.info(f"Log file: {log_file}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
