#!/bin/bash

##############################################################################
# Air Quality Prediction Script with Centralized Configuration
#
# This script generates air quality predictions using the trained XGBoost model
# and processed silver datasets. Supports both real-time and historical modes.
#
# Usage:
#   ./scripts/predict_air_quality.sh --mode realtime --countries THA LAO
#   ./scripts/predict_air_quality.sh --mode historical --countries THA LAO
##############################################################################

# Set up error handling and script directory
set -e  # Exit on any error
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Source configuration and utility functions
if [[ -f "$SCRIPT_DIR/utils/config_reader.sh" ]]; then
    source "$SCRIPT_DIR/utils/config_reader.sh"
else
    echo "Warning: Configuration system not available, using fallbacks"
fi

if [[ -f "$SCRIPT_DIR/utils/common.sh" ]]; then
    source "$SCRIPT_DIR/utils/common.sh"
else
    echo "Warning: Common utilities not available"
    # Define basic logging if common.sh not available
    log_info() { echo "[INFO] $1"; }
    log_warn() { echo "[WARN] $1"; }
    log_error() { echo "[ERROR] $1"; }
    log_success() { echo "[SUCCESS] $1"; }
fi

# Setup environment and error handling (only if functions are available)
if command -v setup_environment &> /dev/null; then
    setup_environment "predict_air_quality"
fi

if command -v setup_error_handling &> /dev/null; then
    setup_error_handling
fi

# Configuration-driven defaults with fallbacks
if command -v get_config_countries &> /dev/null; then
    DEFAULT_COUNTRIES=$(get_config_countries)
    DEFAULT_MODEL_PATH=$(get_config_or_default "models.xgboost" "src/models/xgboost_model.json")
    DEFAULT_MAP_RESOLUTION=$(get_config_or_default "data_processing.h3_resolution" "6")
    DEFAULT_LOG_LEVEL=$(get_config_log_level)
    log_success "Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_COUNTRIES="THA LAO"
    DEFAULT_MODEL_PATH="src/models/xgboost_model.json"
    DEFAULT_MAP_RESOLUTION=6
    DEFAULT_LOG_LEVEL="INFO"
    log_warn "Configuration system unavailable, using hardcoded defaults"
fi

# Default values (config-driven with fallbacks)
MODE="realtime"
COUNTRIES="$DEFAULT_COUNTRIES"
MODEL_PATH="$DEFAULT_MODEL_PATH"
GENERATE_MAP=false
MAP_RESOLUTION="$DEFAULT_MAP_RESOLUTION"
VALIDATE_SENSORS=false
ENHANCED_MAPS=false
SAVE_VALIDATION=false
VALIDATION_OUTPUT_DIR=""

# Function to display usage with configuration-aware defaults
usage() {
    cat << EOF
🔮 Air Quality Prediction Script (Configuration-Aware)

Generate air quality predictions using trained models and silver datasets.
Uses centralized configuration system with backwards-compatible fallbacks.

Usage:
    $0 [OPTIONS]

Mode Arguments:
    --mode MODE              Processing mode: realtime or historical (default: realtime)
    --start-date DATE        Start date for historical mode (YYYY-MM-DD)
    --end-date DATE          End date for historical mode (YYYY-MM-DD)
    --countries CODES        Space-separated country codes (default: $DEFAULT_COUNTRIES)

Prediction Options:
    --model-path PATH        Path to XGBoost model file (default: $DEFAULT_MODEL_PATH)
    --generate-map           Generate AQI prediction maps
    --map-resolution RES     H3 resolution for maps (default: $DEFAULT_MAP_RESOLUTION)

Sensor Validation Options:
    --validate-sensors       Validate predictions against sensor measurements (historical mode only)
    --enhanced-maps          Generate enhanced maps showing both predictions and sensor data
    --save-validation        Save validation results to files
    --validation-output-dir  Directory to save validation outputs (default: data/validation)

General Options:
    --config FILE            Custom configuration file path
    --help                   Display this help message

Configuration Integration:
    This script uses the centralized configuration system when available.
    Configuration defaults are loaded from config/config.yaml.
    Command-line arguments override configuration defaults.

Examples:
    # Real-time prediction with config defaults
    $0

    # Real-time prediction with maps
    $0 --mode realtime --generate-map

    # Historical prediction for specific countries
    $0 --mode historical --countries THA LAO

    # Generate high-resolution prediction maps
    $0 --mode realtime --generate-map --map-resolution 7

    # Use custom model file
    $0 --mode realtime --model-path /path/to/custom_model.json

    # Historical prediction with sensor validation
    $0 --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --validate-sensors --save-validation

    # Historical enhanced prediction with maps and sensor validation
    $0 --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --generate-map --validate-sensors --enhanced-maps --save-validation

Configuration Status:
    Config system: $(command -v get_config_countries &> /dev/null && echo "✅ Available" || echo "❌ Using fallbacks")
    Default countries: $DEFAULT_COUNTRIES
    Default model: $DEFAULT_MODEL_PATH
    Default map resolution: $DEFAULT_MAP_RESOLUTION

Requirements:
    - Silver dataset must exist for the specified mode and countries
    - XGBoost model file must exist at the specified path
    - For map generation: additional processing time required

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --start-date)
            START_DATE="$2"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
            shift 2
            ;;
        --countries)
            COUNTRIES=""
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                COUNTRIES="$COUNTRIES $1"
                shift
            done
            COUNTRIES=$(echo "$COUNTRIES" | sed 's/^ *//')
            ;;
        --model-path)
            MODEL_PATH="$2"
            shift 2
            ;;
        --generate-map|--generate-maps)
            GENERATE_MAP=true
            shift
            ;;
        --map-resolution)
            MAP_RESOLUTION="$2"
            shift 2
            ;;
        --validate-sensors)
            VALIDATE_SENSORS=true
            shift
            ;;
        --enhanced-maps)
            ENHANCED_MAPS=true
            shift
            ;;
        --save-validation)
            SAVE_VALIDATION=true
            shift
            ;;
        --validation-output-dir)
            VALIDATION_OUTPUT_DIR="$2"
            shift 2
            ;;
        --config)
            CONFIG_FILE="$2"
            log_info "Using custom configuration file: $CONFIG_FILE"
            shift 2
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate mode-specific argument combinations early.
if [[ "$MODE" == "realtime" && "$VALIDATE_SENSORS" == true ]]; then
    log_error "validate-sensors is not a valid option for realtime mode"
    exit 2
fi


# Validate model file exists
if [[ ! -f "$BASE_DIR/$MODEL_PATH" ]]; then
    log_error "Model file not found: $BASE_DIR/$MODEL_PATH"
    log_error "Please ensure the XGBoost model file exists at the specified path"
    exit 1
fi

# Build the Python command with configuration integration
read -r -a COUNTRY_ARR <<< "$COUNTRIES"
CMD=(python -m src.predict_air_quality --mode "$MODE")

# Add start/end dates only if provided (for historical mode)
if [[ -n "$START_DATE" ]]; then
    CMD+=(--start-date "$START_DATE")
fi
if [[ -n "$END_DATE" ]]; then
    CMD+=(--end-date "$END_DATE")
fi
CMD+=(--countries "${COUNTRY_ARR[@]}" --model "$MODEL_PATH")

# Add map generation options
if [[ "$GENERATE_MAP" == true ]]; then
    CMD+=(--generate-map --map-resolution "$MAP_RESOLUTION")
fi

# Add sensor validation options
if [[ "$VALIDATE_SENSORS" == true ]]; then
    CMD+=(--validate-sensors)
fi
if [[ "$ENHANCED_MAPS" == true ]]; then
    CMD+=(--enhanced-maps)
fi
if [[ "$SAVE_VALIDATION" == true ]]; then
    CMD+=(--save-validation)
fi
if [[ -n "$VALIDATION_OUTPUT_DIR" ]]; then
    CMD+=(--validation-output-dir "$VALIDATION_OUTPUT_DIR")
fi

# Add custom config file if specified
if [[ -n "$CONFIG_FILE" ]]; then
    CMD+=(--config "$CONFIG_FILE")
fi

# Change to the base directory and run the command
cd "$BASE_DIR"

# Log execution details
log_info "🔮 Starting Air Quality Prediction"
log_info "Mode: $MODE"
if [[ -n "$START_DATE" ]]; then
    log_info "Start date: $START_DATE"
fi
if [[ -n "$END_DATE" ]]; then
    log_info "End date: $END_DATE"
fi
log_info "Countries: $COUNTRIES"
log_info "Model: $MODEL_PATH"
log_info "Generate maps: $GENERATE_MAP"
if [[ "$GENERATE_MAP" == true ]]; then
    log_info "Map resolution: H3 level $MAP_RESOLUTION"
fi
log_info "Validate sensors: $VALIDATE_SENSORS"
if [[ "$ENHANCED_MAPS" == true ]]; then
    log_info "Enhanced maps: $ENHANCED_MAPS"
fi
if [[ "$SAVE_VALIDATION" == true ]]; then
    log_info "Save validation: $SAVE_VALIDATION"
fi
if [[ -n "$VALIDATION_OUTPUT_DIR" ]]; then
    log_info "Validation output directory: $VALIDATION_OUTPUT_DIR"
fi
log_info "Configuration system: $(command -v get_config_countries &> /dev/null && echo "Available" || echo "Fallback mode")"

# Show configuration status
if command -v print_config_summary &> /dev/null; then
    log_info "Configuration summary:"
    print_config_summary | sed 's/^/  /'
fi

# Check for required silver dataset
if command -v get_config_path &> /dev/null; then
    SILVER_DIR=$(get_config_path "silver.$MODE")
    if [[ -d "$SILVER_DIR" ]]; then
        SILVER_FILES=$(find "$SILVER_DIR" -name "*.parquet" | wc -l)
        log_info "Silver dataset: Found $SILVER_FILES files in $SILVER_DIR"
    else
        log_warn "Silver dataset directory not found: $SILVER_DIR"
        log_warn "You may need to run make_silver.sh first"
    fi
fi

log_info "Command: ${CMD[*]}"
log_info ""

run_command_with_timeout() {
    local timeout_seconds="$1"
    shift
    if command -v timeout >/dev/null 2>&1; then
        timeout "$timeout_seconds" "$@"
    else
        "$@"
    fi
}

# Execute the command with enhanced error handling
timeout_seconds=1800  # 30 minutes for prediction
if [[ "$GENERATE_MAP" == true ]]; then
    timeout_seconds=3600  # 1 hour if generating maps
fi

if run_command_with_timeout "$timeout_seconds" "${CMD[@]}"; then
    log_success "Air quality prediction completed successfully"

    # Show output information
    if command -v get_config_path &> /dev/null; then
        PRED_DIR=$(get_config_path "predictions.$MODE")
        if [[ -d "$PRED_DIR" ]]; then
            PRED_FILES=$(find "$PRED_DIR" -name "*.parquet" | wc -l)
            log_success "Prediction files: $PRED_FILES files saved to $PRED_DIR"

            if [[ "$GENERATE_MAP" == true ]]; then
                MAP_DIR=$(get_config_path "predictions.maps.$MODE")
                if [[ -d "$MAP_DIR" ]]; then
                    MAP_FILES=$(find "$MAP_DIR" -name "*.png" | wc -l)
                    log_success "Map files: $MAP_FILES maps saved to $MAP_DIR"
                fi
            fi
        fi
    fi

    exit 0
else
    log_error "Air quality prediction failed"
    exit 1
fi
