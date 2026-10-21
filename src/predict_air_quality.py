#!/usr/bin/env python3
"""
Air Quality Prediction Script

This script loads silver datasets and uses trained XGBoost models to predict PM2.5 air quality
for specified countries and time periods. Supports both real-time and historical prediction modes.
Includes sensor validation capabilities to compare predictions with real sensor measurements.

Usage:
    python src/predict_air_quality.py --mode realtime --countries THA LAO
    python src/predict_air_quality.py --mode historical --start-date 2024-06-01 --end-date 2024-06-03
    python src/predict_air_quality.py --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO --validate-sensors --generate-map
"""



import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import matplotlib.pyplot as plt
import h3
import geopandas as gpd
from shapely.geometry import Polygon

import pandas as pd
import polars as pl
import xgboost as xgb
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import the new configuration system
from src.utils.config_loader import ConfigLoader
from src.utils.logging_utils import setup_with_config
from src.sensor_validation import SensorValidator


def load_model(model_path: str = None, config_loader: ConfigLoader = None, logger=None) -> xgb.XGBRegressor:
    """
    Load the trained XGBoost model from configuration path.
    
    Parameters:
        model_path: Path to model file (None = use config default)
        config_loader: Configuration loader instance
        logger: Logger instance
        
    Returns:
        xgb.XGBRegressor: Loaded model
    """
    if config_loader is None:
        config_loader = ConfigLoader()
    
    if model_path is None:
        # Get model path from config
        model_path = config_loader.get_path('models.xgboost')
        if not model_path:
            # Fallback to default
            model_path = Path("src/models/xgboost_model.json")
    else:
        model_path = Path(model_path)
    
    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found: {model_path}")
    
    # Load model
    model = xgb.XGBRegressor()
    model.load_model(model_path)
    
    if logger:
        logger.info(f"Loaded XGBoost model from: {model_path}")
        logger.info(f"Model feature names: {model.feature_names_in_}")
    
    return model


def load_silver_dataset(
    mode: str,
    countries: List[str],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    config_loader: ConfigLoader = None,
    logger=None
) -> pl.DataFrame:
    """
    Load silver dataset for the specified parameters.
    
    NEW: Loads daily files and combines them (since silver datasets are now saved as one file per day)
    
    Parameters:
        mode: Processing mode ('realtime' or 'historical')
        countries: List of country codes
        start_date: Start date (for historical mode)
        end_date: End date (for historical mode)
        config_loader: Configuration loader instance
        logger: Logger instance
        
    Returns:
        pl.DataFrame: Loaded silver dataset
    """
    if config_loader is None:
        config_loader = ConfigLoader()
    
    # Get silver dataset directory from config
    silver_dir = config_loader.get_path(f'silver.{mode}')
    if not silver_dir or not silver_dir.exists():
        raise FileNotFoundError(f"Silver dataset directory not found: {silver_dir}")
    
    # Create filename pattern for daily files
    countries_str = "_".join(sorted(countries))
    
    if mode == "realtime":
        # Use current date for realtime - load single daily file
        date_str = datetime.now().strftime("%Y%m%d")
        pattern = f"silver_realtime_{countries_str}_{date_str}.parquet"
        
        # Find matching file
        silver_files = list(silver_dir.glob(pattern))
        
        if not silver_files:
            raise FileNotFoundError(f"No silver dataset file found: {pattern} in {silver_dir}")
        
        silver_file = silver_files[0]
        
        if logger:
            logger.info(f"Loading silver dataset from: {silver_file}")
        
        # Load single day
        df = pl.read_parquet(silver_file)
        
    else:
        # Historical mode - load multiple daily files
        if start_date and end_date:
            # Generate list of dates in the range
            date_list = []
            current_date = start_date
            while current_date <= end_date:
                date_list.append(current_date)
                current_date += timedelta(days=1)
            
            if logger:
                logger.info(f"Loading {len(date_list)} daily files for date range: {start_date} to {end_date}")
            
            # Build file patterns for each date
            files_to_load = []
            missing_dates = []
            
            for single_date in date_list:
                date_str = single_date.strftime("%Y%m%d")
                filename = f"silver_historical_{countries_str}_{date_str}.parquet"
                file_path = silver_dir / filename
                
                if file_path.exists():
                    files_to_load.append(file_path)
                else:
                    missing_dates.append(single_date)
            
            if not files_to_load:
                raise FileNotFoundError(
                    f"No silver dataset files found for date range {start_date} to {end_date} "
                    f"in {silver_dir}"
                )
            
            if missing_dates and logger:
                logger.warning(f"Missing files for {len(missing_dates)} dates: {missing_dates[:5]}{'...' if len(missing_dates) > 5 else ''}")
            
            if logger:
                logger.info(f"Found {len(files_to_load)} files to load")
            
            # Use Polars scan_parquet for efficient loading of multiple files
            # Convert Path objects to strings
            file_paths_str = [str(f) for f in files_to_load]
            
            # Load and combine all files
            df = pl.concat([pl.read_parquet(f) for f in file_paths_str])
            
            if logger:
                logger.info(f"Combined {len(files_to_load)} daily files")
        else:
            # No specific date range - load all available files
            pattern = f"silver_historical_{countries_str}_*.parquet"
            silver_files = list(silver_dir.glob(pattern))
            
            if not silver_files:
                raise FileNotFoundError(f"No silver dataset files found matching pattern: {pattern} in {silver_dir}")
            
            if logger:
                logger.info(f"Loading all {len(silver_files)} available files for {countries_str}")
            
            # Load and combine all files
            file_paths_str = [str(f) for f in silver_files]
            df = pl.concat([pl.read_parquet(f) for f in file_paths_str])
    
    if logger:
        logger.info(f"Loaded dataset with shape: {df.shape}")
        logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
        logger.info(f"Countries: {sorted(df['ISO3'].unique().to_list())}")
    
    return df


def prepare_features(df: pl.DataFrame, logger=None) -> Tuple[pd.DataFrame, List[str]]:
    """
    Prepare features for prediction by handling missing values and selecting feature columns.
    
    Parameters:
        df: Polars DataFrame with silver data
        logger: Logger instance
        
    Returns:
        Tuple[pd.DataFrame, List[str]]: Features DataFrame and feature names
    """
    # Model expects these exact features in this order:
    # ['month', 'daily_mean_aod_kriged', 'fire_hotspot_strength', '10u', '10v', '2t', '2d', 'elevation', 'worldpop_population']
    
    # Map silver dataset columns to model features
    # feature_mapping = {
    #     'temperature_2m': '2t',
    #     'wind_u_10m': '10u', 
    #     'wind_v_10m': '10v',
    #     'dewpoint_2m': '2d',
    #     'aod_1day_interpolated': 'daily_mean_aod_kriged',  # Map interpolated AOD to expected column
    #     'fire_hotspot_strength': 'fire_hotspot_strength',
    #     'elevation': 'elevation',
    #     'worldpop_population': 'worldpop_population'
    # }
    
    # Convert to pandas and add month feature
    # feature_df = df.select(['cell', 'date', 'ISO3'] + list(feature_mapping.keys())).to_pandas()
    
    # Add month feature from date
    feature_df=df.to_pandas()
    feature_df['month'] = feature_df['date'].dt.month
    
    # Rename columns to match model expectations
    # feature_df = feature_df.rename(columns=feature_mapping)
    
    # Define the exact features the model expects
    model_features = ["month","worldpop_population","aod_1day_interpolated","fire_hotspot_strength","temperature_2m","wind_u_10m","wind_v_10m","dewpoint_2m","yesterday_parent_h3_04_pm25","aod_1day_interpolated_roll3","aod_1day_interpolated_roll7","fire_hotspot_strength_roll3","fire_hotspot_strength_roll7","temperature_2m_roll3","temperature_2m_roll7","dewpoint_2m_roll3","dewpoint_2m_roll7","wind_u_10m_roll3","wind_u_10m_roll7","wind_v_10m_roll3","wind_v_10m_roll7"]
    
    # Check for missing features
    missing_features = [col for col in model_features if col not in feature_df.columns]
    available_features = [col for col in model_features if col in feature_df.columns]
    
    if logger:
        logger.info(f"Available features ({len(available_features)}): {available_features}")
        if missing_features:
            logger.warning(f"Missing features ({len(missing_features)}): {missing_features}")
    
    # Handle missing values with appropriate strategies
    if logger:
        null_counts = feature_df[available_features].isnull().sum()
        if null_counts.sum() > 0:
            logger.info("Null value counts by feature:")
            for col, count in null_counts.items():
                if count > 0:
                    logger.info(f"  {col}: {count}")
    
    # Fill missing values
    for col in available_features:
        if col == 'fire_hotspot_strength':
            # Fill fire data with 0 (no fires)
            feature_df[col] = feature_df[col].fillna(0.0)
        elif col == 'daily_mean_aod_kriged':
            # Leave AOD as null - this indicates missing satellite data
            pass
        elif col in ['2t', '10u', '10v', '2d']:
            # Fill weather data with column mean
            feature_df[col] = feature_df[col].fillna(feature_df[col].mean())
        elif col == 'elevation':
            # Fill elevation with median
            feature_df[col] = feature_df[col].fillna(feature_df[col].median())
        elif col == 'worldpop_population':
            # Fill population with 0
            feature_df[col] = feature_df[col].fillna(0.0)
        elif col == 'month':
            # Month should never be null since we derive it from date
            pass
    
    if logger:
        logger.info(f"Features prepared for {len(feature_df)} records")
        logger.info(f"Feature statistics:")
        logger.info(feature_df[available_features].describe().round(3))
    
    return feature_df, available_features


def make_predictions(
    model: xgb.XGBRegressor,
    feature_df: pd.DataFrame,
    feature_columns: List[str],
    logger=None
) -> pd.DataFrame:
    """
    Make PM2.5 predictions using the trained model.
    
    Parameters:
        model: Trained XGBoost model
        feature_df: Features DataFrame
        feature_columns: List of feature column names
        logger: Logger instance
        
    Returns:
        pd.DataFrame: DataFrame with predictions
    """
    # Prepare features for prediction
    X = feature_df[feature_columns].values
    
    # Make predictions
    predictions = model.predict(X)

    # exponential the predictions as the model predicts in log mode
    predictions = np.expm1(predictions)   
    
    # Clip predictions to reasonable range (0-500 μg/m³)
    predictions = np.clip(predictions, 0, 500)
    
    # Create results DataFrame
    results_df = feature_df[['cell', 'date', 'ISO3']].copy()
    results_df['predicted_pm25'] = predictions
    
    if logger:
        logger.info(f"Generated {len(predictions)} predictions")
        logger.info(f"Prediction statistics:")
        logger.info(f"  Min: {predictions.min():.2f} μg/m³")
        logger.info(f"  Max: {predictions.max():.2f} μg/m³")
        logger.info(f"  Mean: {predictions.mean():.2f} μg/m³")
        logger.info(f"  Median: {np.median(predictions):.2f} μg/m³")
    
    return results_df


def save_predictions(
    predictions_df: pd.DataFrame,
    mode: str,
    countries: List[str],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    config_loader: ConfigLoader = None,
    logger=None
) -> str:
    """
    Save predictions to the appropriate directory using configuration paths.
    
    NEW BEHAVIOR: Saves one file per day (not one file for entire range)
    
    Parameters:
        predictions_df: DataFrame with predictions
        mode: Processing mode ('realtime' or 'historical')
        countries: List of country codes
        start_date: Start date (for historical mode)
        end_date: End date (for historical mode)
        config_loader: Configuration loader instance
        logger: Logger instance
        
    Returns:
        str: Path to output directory (contains multiple daily files)
    """
    if config_loader is None:
        config_loader = ConfigLoader()
    
    # Create output directory using config paths
    output_dir = config_loader.get_path(f'predictions.data.{mode}', create_if_missing=True)
    
    # Generate filename pattern
    countries_str = "_".join(sorted(countries))
    
    # Get unique dates in the predictions DataFrame
    unique_dates = sorted(pd.to_datetime(predictions_df['date']).dt.date.unique())
    
    if logger:
        logger.info(f"Saving {len(unique_dates)} daily prediction files to: {output_dir}")
    
    # Save one file per day
    saved_files = []
    for single_date in unique_dates:
        # Filter to this specific date
        daily_df = predictions_df[pd.to_datetime(predictions_df['date']).dt.date == single_date]
        
        # Generate filename for this date
        date_str = single_date.strftime("%Y%m%d")
        if mode == "realtime":
            filename = f"aq_predictions_{date_str}_{countries_str}.parquet"
        else:
            filename = f"aq_predictions_{date_str}_{countries_str}.parquet"
        
        output_path = output_dir / filename
        
        # Remove existing file if it exists
        if output_path.exists():
            output_path.unlink()
        
        # Save this day's predictions
        daily_df.to_parquet(output_path, index=False)
        
        saved_files.append(str(output_path))
        
        if logger:
            n_rows = len(daily_df)
            logger.info(f"  Saved {single_date}: {output_path.name} ({n_rows:,} predictions)")
    
    if logger:
        total_predictions = len(predictions_df)
        logger.info(f"Successfully saved {len(saved_files)} daily prediction files")
        logger.info(f"Total predictions: {total_predictions:,} records")
    
    # Return the output directory path
    return str(output_dir)



def generate_daily_maps_and_charts(
    predictions_df: pd.DataFrame,
    mode: str,
    countries: List[str],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    map_resolution: int = 6,
    config_loader: ConfigLoader = None,
    logger=None
):
    """
    Generate maps and charts for each date in the predictions.
    
    Parameters:
        predictions_df: DataFrame with predictions
        mode: Processing mode ('realtime' or 'historical')
        countries: List of country codes
        start_date: Start date (for historical mode)
        end_date: End date (for historical mode)
        map_resolution: H3 resolution for map generation
        config_loader: Configuration loader instance
        logger: Logger instance
    """
    if config_loader is None:
        config_loader = ConfigLoader()
    
    # Get unique dates from predictions
    unique_dates = sorted(predictions_df['date'].dt.date.unique())
    
    if logger:
        logger.info(f"Generating maps and charts for {len(unique_dates)} dates: {unique_dates}")
    
    # Create output directories
    map_dir = Path(f'data/predictions/map/{mode}')
    map_dir.mkdir(parents=True, exist_ok=True)
    
    countries_str = "_".join(sorted(countries))
    
    # Generate maps and charts for each date
    for current_date in unique_dates:
        try:
            # Filter predictions for current date
            date_predictions = predictions_df[predictions_df['date'].dt.date == current_date].copy()
            
            if date_predictions.empty:
                logger.warning(f"No predictions found for date: {current_date}")
                continue
                
            if logger:
                logger.info(f"Processing date: {current_date} ({len(date_predictions)} predictions)")
            
            # Generate map for this date
            generate_map_for_date(
                date_predictions, current_date, countries_str, 
                map_dir, map_resolution, logger
            )
            
            # Generate chart for this date
            generate_chart_for_date(
                date_predictions, current_date, countries_str, 
                countries, logger
            )
            
        except Exception as e:
            logger.error(f"Failed to generate map/chart for date {current_date}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())


def generate_map_for_date(
    date_predictions: pd.DataFrame,
    current_date: date,
    countries_str: str,
    map_dir: Path,
    map_resolution: int,
    logger=None
):
    """Generate a map for a specific date."""

    
    try:
        # Generate filename with the actual date
        date_str = current_date.strftime("%Y%m%d")
        map_filename = f"aqi_map_{date_str}_{countries_str}.png"
        map_path = map_dir / map_filename
        
        if logger:
            logger.info(f"Generating map: {map_filename}")
        
        # WHO AQI categories and colors
        AQI_BREAKS = [0, 9.0, 35.4, 55.4, 125.4, 225.4, float("inf")]
        AQI_LABELS = ["1.Good", "2.Moderate", "3.USG", "4.Unhealthy", "5.Very Unhealthy", "6.Hazardous"]
        AQI_COLORS = {
            "1.Good": "#a8e05f",
            "2.Moderate": "#fdd64b", 
            "3.USG": "#ff9b57",
            "4.Unhealthy": "#fe6a69",
            "5.Very Unhealthy": "#a97abc",
            "6.Hazardous": "#a87383"
        }
        
        # Aggregate data at lower resolution
        date_predictions['h3_res_low'] = date_predictions['cell'].apply(
            lambda x: h3.h3_to_parent(h3.h3_to_string(x), map_resolution)
        )
        
        # Take mean PM2.5 values per aggregated hexagon
        df_agg = date_predictions.groupby('h3_res_low', as_index=False)['predicted_pm25'].mean()
        
        if logger:
            logger.info(f"Aggregated from {len(date_predictions)} cells to {len(df_agg)} hexagons")
        
        # Convert aggregated H3 cells to hexagon geometries
        hexagons = []
        pm25_values = []
        
        for _, row in df_agg.iterrows():
            try:
                h3_id = row['h3_res_low']  # Already a string from h3_to_parent
                # Get hexagon boundary
                boundary = h3.h3_to_geo_boundary(h3_id, geo_json=True)
                # Create polygon (h3 geo_json=True returns coords as [lng, lat])
                coords = [(pt[0], pt[1]) for pt in boundary]
                hexagon = Polygon(coords)
                hexagons.append(hexagon)
                pm25_values.append(row['predicted_pm25'])
            except Exception as e:
                continue
        
        if hexagons:
            # Create GeoDataFrame
            gdf_hex = gpd.GeoDataFrame({
                'PM25': pm25_values,
                'geometry': hexagons
            })
            
            # Assign AQI categories
            def assign_aqi_category(pm25):
                for i, break_point in enumerate(AQI_BREAKS[1:]):
                    if pm25 <= break_point:
                        return AQI_LABELS[i]
                return AQI_LABELS[-1]
            
            gdf_hex['AQI_Category'] = gdf_hex['PM25'].apply(assign_aqi_category)
            
            # Create the map
            fig, ax = plt.subplots(figsize=(10, 8))
            
            # Plot hexagons by AQI category and create custom legend
            legend_handles = []
            for category in AQI_LABELS:
                subset = gdf_hex[gdf_hex["AQI_Category"] == category]
                if not subset.empty:
                    subset.plot(
                        ax=ax,
                        color=AQI_COLORS[category],
                        edgecolor=None,  # Remove edges for solid appearance
                        alpha=1.0  # Fully opaque
                    )
                    # Create custom legend handle
                    from matplotlib.patches import Patch
                    legend_handles.append(Patch(facecolor=AQI_COLORS[category], label=category))
            
            # Customize with the actual date
            ax.set_title(f"PM2.5 AQI Map - {countries_str.replace('_', ', ')} - {date_str}", fontsize=16)
            ax.axis("off")
            
            # Add custom legend
            if legend_handles:
                ax.legend(handles=legend_handles, loc="lower left", fontsize=9, frameon=True)
            
            plt.tight_layout()
            
            # Save map
            plt.savefig(map_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            if logger:
                logger.info(f"Map saved: {map_path}")
        else:
            logger.warning(f"No valid hexagons for map generation for date: {current_date}")
            
    except Exception as e:
        logger.error(f"Failed to generate map for date {current_date}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


def generate_chart_for_date(
    date_predictions: pd.DataFrame,
    current_date: date,
    countries_str: str,
    countries: List[str],
    logger=None
):
    """Generate a distribution chart for a specific date."""
    try:
        import subprocess
        
        # Create a temporary file for this date's data
        temp_dir = Path('temp_predictions')
        temp_dir.mkdir(exist_ok=True)
        
        date_str = current_date.strftime("%Y%m%d")
        temp_file = temp_dir / f"temp_predictions_{date_str}_{countries_str}.parquet"
        
        # Save date-specific predictions
        date_predictions.to_parquet(temp_file, index=False)
        
        if logger:
            logger.info(f"Generating distribution chart for date: {current_date}")
        
        # Call the existing plot script with the date-specific file
        result = subprocess.run([
            'python', 'src/plot_pm25_distribution.py', 
            '--input-file', str(temp_file),
            '--countries'] + countries,
            capture_output=True, text=True)
        
        result = subprocess.run([
            'python', 'src/plot_pm25_distribution.py', 
            '--input-file', str(temp_file),
            '--countries'] + countries + [
            '--target-date', current_date.strftime("%Y-%m-%d")  
        ],
            capture_output=True, text=True)
                
        if result.returncode == 0:
            if logger:
                logger.info(f"Distribution chart generated successfully for date: {current_date}")
        else:
            if logger:
                logger.error(f"Failed to generate distribution chart for date {current_date}: {result.stderr}")
        
        # Clean up temp file
        if temp_file.exists():
            temp_file.unlink()
        
        # Remove temp directory if empty
        if temp_dir.exists() and not any(temp_dir.iterdir()):
            temp_dir.rmdir()
            
    except Exception as e:
        logger.error(f"Failed to generate distribution chart for date {current_date}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


def generate_enhanced_realtime_map(
    predictions_df: pd.DataFrame,
    sensor_data: pd.DataFrame,
    validation_results: pd.DataFrame,
    sensor_validator,
    countries: List[str],
    map_resolution: int,
    logger=None
):
    """Generate enhanced map for realtime mode."""
    if sensor_validator and sensor_data is not None and not sensor_data.empty:
        # Prepare matched data for enhanced maps
        matched_data = validation_results if validation_results is not None and not validation_results.empty else pd.DataFrame()
        
        # Generate enhanced map using existing sensor_validator method
        map_path = sensor_validator.create_enhanced_map(
            prediction_df=predictions_df,
            sensor_df=sensor_data,
            matched_df=matched_data,
            countries=countries,
            target_date=datetime.now().date(),
            map_resolution=map_resolution,
            output_path=None  # Let the validator determine the path
        )
        
        if map_path:
            logger.info(f"Enhanced realtime map generated: {map_path}")
        else:
            logger.warning("Enhanced realtime map generation failed")
    else:
        logger.warning("No sensor data available for enhanced realtime map. Generating standard map instead.")
        # Fall back to standard map
        generate_map_for_date(
            predictions_df, datetime.now().date(), "_".join(sorted(countries)),
            Path(f'data/predictions/map/realtime'), map_resolution, logger
        )

def generate_enhanced_historical_maps(
    predictions_df: pd.DataFrame,
    countries: List[str],
    start_date: Optional[date],
    end_date: Optional[date],
    map_resolution: int,
    sensor_validator=None,
    config_loader: ConfigLoader = None,
    logger=None
):
    """Generate enhanced maps for historical mode - one per date with available sensor data."""
    
    # Get unique dates from predictions
    unique_dates = sorted(predictions_df['date'].dt.date.unique())
    countries_str = "_".join(sorted(countries))
    
    # Create output directory
    map_dir = Path(f'data/predictions/map/historical')
    map_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Generating enhanced historical maps for {len(unique_dates)} dates")
    
    enhanced_maps_generated = 0
    standard_maps_generated = 0
    scatter_plots_generated = 0
    
    for current_date in unique_dates:
        try:
            # Filter predictions for current date
            date_predictions = predictions_df[predictions_df['date'].dt.date == current_date].copy()
            
            if date_predictions.empty:
                logger.warning(f"No predictions found for date: {current_date}")
                continue
                
            logger.info(f"Processing enhanced map for date: {current_date}")
            
            # Try to load sensor data for this date
            date_sensor_data = pd.DataFrame()
            validation_results_for_date = pd.DataFrame()
            
            if sensor_validator:
                try:
                    date_sensor_data = sensor_validator.load_sensor_data(
                        countries=countries,
                        target_date=current_date,
                        mode="historical"  # This function is for historical mode
                    )
                    
                    if not date_sensor_data.empty:
                        logger.info(f"Loaded {len(date_sensor_data)} sensor measurements for {current_date}")
                        
                        # Perform validation for enhanced visualization
                        predictions_for_validation = date_predictions.copy()
                        
                        # Convert H3 integer IDs to string format
                        import h3
                        predictions_for_validation['h3_08'] = predictions_for_validation['cell'].apply(
                            lambda x: h3.h3_to_string(int(x)) if pd.notna(x) else None
                        )
                        
                        # Add AQI categories
                        def assign_aqi_category(pm25_value):
                            AQI_BREAKS = [0, 9.0, 35.4, 55.4, 125.4, 225.4, float("inf")]
                            AQI_LABELS = ["1.Good", "2.Moderate", "3.USG", "4.Unhealthy", "5.Very Unhealthy", "6.Hazardous"]
                            for i, break_point in enumerate(AQI_BREAKS[1:]):
                                if pm25_value <= break_point:
                                    return AQI_LABELS[i]
                            return AQI_LABELS[-1]
                        
                        predictions_for_validation['predicted_aqi_category'] = predictions_for_validation['predicted_pm25'].apply(assign_aqi_category)
                        
                        # Validate predictions against sensors
                        matched_df, validation_metrics = sensor_validator.validate_predictions(
                            sensor_df=date_sensor_data,
                            prediction_df=predictions_for_validation
                        )
                        
                        if not matched_df.empty:
                            validation_results_for_date = matched_df
                            logger.info(f"Validation for {current_date}: {len(matched_df)} matched locations")
                            
                            # Generate scatter plot for this specific date
                            scatter_path = sensor_validator.create_scatter_plot(
                                matched_df=matched_df,
                                metrics=validation_metrics,
                                countries=countries,
                                target_date=current_date  # Use actual date instead of datetime.now().date()
                            )
                            if scatter_path:
                                logger.info(f"Scatter plot generated for {current_date}: {scatter_path}")
                                scatter_plots_generated += 1
                            else:
                                logger.warning(f"Scatter plot generation failed for {current_date}")
                        
                    else:
                        logger.info(f"No sensor data available for {current_date}")
                        
                except Exception as e:
                    logger.warning(f"Failed to load sensor data for {current_date}: {str(e)}")
            
            # Generate enhanced map if we have sensor data, otherwise standard map
            if not date_sensor_data.empty and sensor_validator:
                try:
                    # Generate enhanced map using sensor_validator
                    map_path = sensor_validator.create_enhanced_map(
                        prediction_df=date_predictions,
                        sensor_df=date_sensor_data,
                        matched_df=validation_results_for_date,
                        countries=countries,
                        target_date=current_date,
                        map_resolution=map_resolution,
                        output_path=None
                    )
                    
                    if map_path:
                        logger.info(f"Enhanced map generated for {current_date}: {map_path}")
                        enhanced_maps_generated += 1
                    else:
                        raise Exception("Enhanced map generation returned None")
                        
                except Exception as e:
                    logger.warning(f"Enhanced map generation failed for {current_date}: {str(e)}")
                    logger.info(f"Falling back to standard map for {current_date}")
                    
                    # Fall back to standard map
                    generate_map_for_date(
                        date_predictions, current_date, countries_str,
                        map_dir, map_resolution, logger
                    )
                    standard_maps_generated += 1
            else:
                # Generate standard map (no sensor data available)
                logger.info(f"Generating standard map for {current_date} (no sensor data)")
                generate_map_for_date(
                    date_predictions, current_date, countries_str,
                    map_dir, map_resolution, logger
                )
                standard_maps_generated += 1
            
            # Generate distribution chart for this date regardless of sensor data availability
            generate_chart_for_date(
                date_predictions, current_date, countries_str,
                countries, logger
            )
            
        except Exception as e:
            logger.error(f"Failed to process enhanced map for date {current_date}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    logger.info(f"Enhanced historical maps summary:")
    logger.info(f"  Enhanced maps generated: {enhanced_maps_generated}")
    logger.info(f"  Standard maps generated: {standard_maps_generated}")
    logger.info(f"  Scatter plots generated: {scatter_plots_generated}")
    logger.info(f"  Total dates processed: {len(unique_dates)}")



def main():
    """Main entry point for air quality prediction with configuration integration."""
    # Load configuration for defaults
    try:
        config_loader = ConfigLoader()
        default_countries = config_loader.get_countries()
        logging_config = config_loader.get_logging_config()
        default_log_level = logging_config.get('level', 'INFO')
    except Exception as e:
        print(f"Warning: Could not load configuration, using fallback defaults: {e}")
        default_countries = ["THA", "LAO"]
        default_log_level = "INFO"
    
    parser = argparse.ArgumentParser(
        description="Air Quality Prediction System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Realtime predictions using latest data
  python src/predict_air_quality.py --mode realtime --countries THA LAO

  # Historical predictions for specific date range
  python src/predict_air_quality.py --mode historical --start-date 2024-02-01 --end-date 2024-02-02

  # Single date historical prediction
  python src/predict_air_quality.py --mode historical --start-date 2024-02-01

  # Use custom configuration file
  python src/predict_air_quality.py --mode realtime --config config/custom_config.yaml

  # Use custom model file
  python src/predict_air_quality.py --mode realtime --model src/models/custom_model.json
        """
    )
    
    parser.add_argument('--mode', choices=['historical', 'realtime'], required=True,
                       help='Prediction mode: historical or realtime')
    
    parser.add_argument('--start-date', type=str,
                       help='Start date for historical mode (YYYY-MM-DD), required for historical mode')
    
    parser.add_argument('--end-date', type=str, 
                       help='End date for historical mode (YYYY-MM-DD), optional')
    
    parser.add_argument('--countries', nargs='+', default=default_countries,
                       help=f'Country codes (default: {" ".join(default_countries)})')
    
    parser.add_argument('--model', type=str,
                       help='Path to model file (optional, uses config default)')
    
    parser.add_argument('--config', type=str,
                       help='Path to configuration file (optional)')
    
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default=default_log_level, help=f'Logging level (default: {default_log_level})')
    
    parser.add_argument('--generate-map', action='store_true',
                       help='Generate standard AQI prediction maps')
    
    parser.add_argument('--map-resolution', type=int, default=8,
                       help='H3 resolution for map generation (default: 8)')
    
    # Sensor validation arguments
    parser.add_argument('--validate-sensors', action='store_true',
                       help='Validate predictions against sensor measurements (historical mode only)')
    
    parser.add_argument('--enhanced-maps', action='store_true',
                       help='Generate enhanced maps showing both predictions and sensor data')
    
    parser.add_argument('--save-validation', action='store_true',
                       help='Save validation results to files')
    
    parser.add_argument('--validation-output-dir', type=str,
                       help='Directory to save validation outputs (default: data/validation)')
    
    args = parser.parse_args()
    
    # Initialize configuration loader with custom config if provided
    if args.config:
        config_loader = ConfigLoader(args.config)
    else:
        config_loader = ConfigLoader()
    
    # Setup logging using configuration
    logger = setup_with_config(config_loader, __name__)
    logger.setLevel(getattr(logging, args.log_level))
    
    # Validate arguments
    if args.mode == "historical":
        if not args.start_date:
            parser.error("Historical mode requires --start-date")
        
        # Validate date format
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date() if args.end_date else start_date
        except ValueError:
            parser.error("Date format must be YYYY-MM-DD")
    else:
        start_date = None
        end_date = None

    if args.mode == "realtime" and args.validate_sensors:
        parser.error("validate-sensors is not a valid option for realtime mode")
    
    logger.info("="*80)
    logger.info("AIR QUALITY PREDICTION SYSTEM STARTING")
    logger.info("="*80)
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Countries: {', '.join(args.countries)}")
    if args.mode == "historical":
        logger.info(f"Date range: {start_date} to {end_date}")
    
    try:
        # Load model
        logger.info("Loading XGBoost model...")
        model = load_model(args.model, config_loader, logger)
        
        # Load silver dataset
        logger.info("Loading silver dataset...")
        silver_df = load_silver_dataset(
            args.mode, args.countries, start_date, end_date, config_loader, logger
        )
        
        # Prepare features
        logger.info("Preparing features for prediction...")
        feature_df, feature_columns = prepare_features(silver_df, logger)
        
        # Make predictions
        logger.info("Making predictions...")
        predictions_df = make_predictions(model, feature_df, feature_columns, logger)
        
        # Save predictions
        logger.info("Saving predictions...")
        output_path = save_predictions(
            predictions_df, args.mode, args.countries, start_date, end_date, config_loader, logger
        )
        
        # Initialize sensor validation variables
        sensor_validator = None
        sensor_data = None
        validation_results = None
        
        # Sensor validation workflow
        # Note: Validation only makes sense in historical mode (can't validate predictions for future dates)
        if (args.validate_sensors or args.enhanced_maps) and args.mode == "historical":
            logger.info("="*80)
            logger.info("SENSOR VALIDATION")
            logger.info("="*80)
            
            try:
                # Initialize sensor validator
                sensor_validator = SensorValidator(
                    config_path=args.config if hasattr(args, 'config') else None
                )
                
                # Convert predictions to proper format for validation
                predictions_for_validation = predictions_df.copy()
                
                # Convert H3 integer IDs to string format to match sensor data
                import h3
                predictions_for_validation['h3_08'] = predictions_for_validation['cell'].apply(
                    lambda x: h3.h3_to_string(int(x)) if pd.notna(x) else None
                )
                
                # Add AQI category for predictions using the same categories as SensorValidator
                def assign_aqi_category(pm25_value):
                    AQI_BREAKS = [0, 9.0, 35.4, 55.4, 125.4, 225.4, float("inf")]
                    AQI_LABELS = ["1.Good", "2.Moderate", "3.USG", "4.Unhealthy", "5.Very Unhealthy", "6.Hazardous"]
                    for i, break_point in enumerate(AQI_BREAKS[1:]):
                        if pm25_value <= break_point:
                            return AQI_LABELS[i]
                    return AQI_LABELS[-1]
                
                predictions_for_validation['predicted_aqi_category'] = predictions_for_validation['predicted_pm25'].apply(assign_aqi_category)
                
                # Load sensor data
                logger.info("Loading sensor data for validation...")
                try:
                    if args.mode == "realtime":
                        # For realtime mode, use current date
                        sensor_data = sensor_validator.load_sensor_data(
                            countries=args.countries,
                            target_date=datetime.now().date(),
                            mode="realtime"
                        )
                        
                        if sensor_data.empty:
                            logger.warning("No sensor data found for realtime validation")
                        else:
                            logger.info(f"Loaded sensor data: {len(sensor_data)} measurements")
                            
                            # Perform validation
                            logger.info("Performing validation against sensor measurements...")
                            matched_df, validation_metrics = sensor_validator.validate_predictions(
                                sensor_df=sensor_data,
                                prediction_df=predictions_for_validation
                            )
                            
                            if not matched_df.empty:
                                logger.info(f"Validation completed: {len(matched_df)} matched locations")
                                
                                # Save validation results if requested
                                if args.save_validation:
                                    validation_output_path = sensor_validator.save_validation_results(
                                        matched_df, validation_metrics, args.countries, 
                                        target_date=datetime.now().date()
                                    )
                                    if validation_output_path:
                                        logger.info(f"Validation results saved to: {validation_output_path}")
                                
                                # Generate scatter plot for realtime
                                logger.info("Generating validation scatter plot...")
                                scatter_path = sensor_validator.create_scatter_plot(
                                    matched_df=matched_df,
                                    metrics=validation_metrics,
                                    countries=args.countries,
                                    target_date=datetime.now().date()
                                )
                                if scatter_path:
                                    logger.info(f"Validation scatter plot saved to: {scatter_path}")
                                else:
                                    logger.warning("Scatter plot generation failed")
                                
                                # Store for map generation
                                validation_results = matched_df
                            else:
                                logger.warning("No matching locations found between predictions and sensors")
                                validation_results = pd.DataFrame()
                    else:
                        # For historical mode, scatter plots will be generated per date in enhanced_historical_maps
                        logger.info("Historical mode: Scatter plots will be generated per date during map generation")
                        
                except Exception as e:
                    logger.error(f"Error loading sensor data: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    sensor_data = pd.DataFrame()
                        
            except Exception as e:
                logger.error(f"Sensor validation failed: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
        
        # Generate maps and charts
        if args.generate_map or args.enhanced_maps:
            logger.info("="*80)
            logger.info("MAP AND CHART GENERATION")
            logger.info("="*80)
            
            try:
                # Always generate standard maps and distribution charts if --generate-map is set
                if args.generate_map:
                    logger.info("Generating standard AQI prediction maps and charts for each date...")
                    
                    generate_daily_maps_and_charts(
                        predictions_df=predictions_df,
                        mode=args.mode,
                        countries=args.countries,
                        start_date=start_date,
                        end_date=end_date,
                        map_resolution=args.map_resolution,
                        config_loader=config_loader,
                        logger=logger
                    )
                
                # Generate enhanced maps if --enhanced-maps is set
                if args.enhanced_maps:
                    logger.info("Generating enhanced maps with predictions and sensor data...")
                    
                    if args.mode == "realtime":
                        # Realtime enhanced map (single map with current sensor data)
                        generate_enhanced_realtime_map(
                            predictions_df=predictions_df,
                            sensor_data=sensor_data if 'sensor_data' in locals() and not sensor_data.empty else None,
                            validation_results=validation_results if 'validation_results' in locals() and not validation_results.empty else None,
                            sensor_validator=sensor_validator if 'sensor_validator' in locals() else None,
                            countries=args.countries,
                            map_resolution=args.map_resolution,
                            logger=logger
                        )
                    else:
                        # Historical enhanced maps (one map per date with available sensor data)
                        generate_enhanced_historical_maps(
                            predictions_df=predictions_df,
                            countries=args.countries,
                            start_date=start_date,
                            end_date=end_date,
                            map_resolution=args.map_resolution,
                            sensor_validator=sensor_validator if 'sensor_validator' in locals() else None,
                            config_loader=config_loader,
                            logger=logger
                        )
                        
            except Exception as e:
                logger.error(f"Failed to generate maps/charts: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
        elif not args.enhanced_maps and not args.generate_map:
            # Always generate distribution charts even if no maps are requested
            logger.info("="*80)
            logger.info("DISTRIBUTION CHART GENERATION")
            logger.info("="*80)
            logger.info("Generating distribution charts for each date...")
            
            # Get unique dates from predictions
            unique_dates = sorted(predictions_df['date'].dt.date.unique())
            countries_str = "_".join(sorted(args.countries))
            
            for current_date in unique_dates:
                try:
                    # Filter predictions for current date
                    date_predictions = predictions_df[predictions_df['date'].dt.date == current_date].copy()
                    
                    if date_predictions.empty:
                        logger.warning(f"No predictions found for date: {current_date}")
                        continue
                    
                    # Generate distribution chart for this date
                    generate_chart_for_date(
                        date_predictions, current_date, countries_str,
                        args.countries, logger
                    )
                    
                except Exception as e:
                    logger.error(f"Failed to generate distribution chart for date {current_date}: {str(e)}")

        
        # Summary statistics
        logger.info("="*80)
        logger.info("PREDICTION SUMMARY")
        logger.info("="*80)
        logger.info(f"Total predictions: {len(predictions_df):,}")
        logger.info(f"Date range: {predictions_df['date'].min()} to {predictions_df['date'].max()}")
        logger.info(f"Countries: {sorted(predictions_df['ISO3'].unique())}")
        logger.info(f"H3 cells: {predictions_df['cell'].nunique():,}")
        logger.info(f"Average PM2.5: {predictions_df['predicted_pm25'].mean():.2f} μg/m³")
        logger.info(f"Output saved to: {output_path}")
        
        # Add map generation summary
        if args.generate_map or args.enhanced_maps:
            logger.info("")
            unique_dates = sorted(predictions_df['date'].dt.date.unique())
            
            if args.generate_map:
                logger.info(f"STANDARD MAPS: Generated {len(unique_dates)} maps for dates: {unique_dates}")
                logger.info(f"DISTRIBUTION CHARTS: Generated {len(unique_dates)} charts for dates: {unique_dates}")
            
            if args.enhanced_maps:
                if args.mode == "realtime":
                    logger.info("ENHANCED MAP: Generated with predictions and sensor overlay")
                else:
                    logger.info(f"ENHANCED MAPS: Generated for {len(unique_dates)} dates: {unique_dates}")
        else:
            # Distribution charts were generated even without maps
            unique_dates = sorted(predictions_df['date'].dt.date.unique())
            logger.info("")
            logger.info(f"DISTRIBUTION CHARTS: Generated {len(unique_dates)} charts for dates: {unique_dates}")
        
        logger.info("="*80)
        logger.info("AIR QUALITY PREDICTION COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        
    except Exception as e:
        logger.error("="*80)
        logger.error("AIR QUALITY PREDICTION FAILED")
        logger.error(f"Error: {str(e)}")
        logger.error("="*80)
        import traceback
        logger.error(traceback.format_exc())
        exit(1)


if __name__ == "__main__":
    main()
