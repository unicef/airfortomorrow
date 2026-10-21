#!/bin/bash

# OpenAQ Historical Data Collection Script
# This script collects historical air quality data from OpenAQ S3 bucket for Thailand and Laos
# It's designed to be run as needed or scheduled via cron

# Set the base directory to the script location
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$BASE_DIR"

# Set up script directory for configuration access
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source configuration and utility functions
CONFIG_AVAILABLE=false
if [[ -f "$SCRIPT_DIR/utils/config_reader.sh" ]]; then
    # Check if yq is available before sourcing
    if command -v yq &> /dev/null; then
        if source "$SCRIPT_DIR/utils/config_reader.sh" 2>/dev/null; then
            CONFIG_AVAILABLE=true
        else
            echo "⚠️  Configuration system failed to load, using fallbacks"
        fi
    else
        echo "⚠️  yq not available, using fallback defaults"
    fi
else
    echo "⚠️  Configuration system not available, using fallbacks"
fi

# Set PYTHONPATH to include the project root directory
export PYTHONPATH="$BASE_DIR:$PYTHONPATH"


# Create logs directory if it doesn't exist
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    LOG_DIR=$(get_config_path "logs" "./logs")
else
    LOG_DIR="$BASE_DIR/logs"
fi
mkdir -p "$LOG_DIR"

# Set log file with timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/openaq_historical_$TIMESTAMP.log"

# Configuration-driven defaults with fallbacks
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    # Get numeric country codes for OpenAQ API from configuration
    OPENAQ_CODES_STR=$(get_config_countries "openaq")
    OPENAQ_CODES=($OPENAQ_CODES_STR)
    DEFAULT_MAX_WORKERS=$(get_config_max_workers)
    DEFAULT_OUTPUT_DIR=$(get_config_path "raw.openaq.historical" "$BASE_DIR/data/raw/openaq/historical")
    DEFAULT_TIMEOUT=$(get_config_timeout "download_timeout")
    [[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    OPENAQ_CODES=(68 111)  # LAO=68, THA=111
    DEFAULT_MAX_WORKERS=4
    DEFAULT_OUTPUT_DIR="$BASE_DIR/data/raw/openaq/historical"
    DEFAULT_TIMEOUT=1800
    echo "⚠️  Configuration system unavailable, using hardcoded defaults"
fi

# Default settings (config-driven with fallbacks)
YEARS=(2023 2024)
COUNTRY_CODES=("${OPENAQ_CODES[@]}")
MAX_WORKERS="$DEFAULT_MAX_WORKERS"
MAX_RETRIES=3
OUTPUT_DIR="$DEFAULT_OUTPUT_DIR"
TEST_MODE=false
TIMEOUT="$DEFAULT_TIMEOUT"

if [[ "${AIR_QUALITY_TEST_MODE:-0}" == "1" ]]; then
    TEST_MODE=true
fi

# Function to log messages
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_FILE"
}

# Print usage information
usage() {
    echo "OpenAQ Historical Data Collection Script (Configuration-Aware)"
    echo ""
    echo "This script collects historical air quality data from OpenAQ S3 bucket for Thailand and Laos"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -y, --years YEARS         Years to download (space-separated, default: ${YEARS[*]})"
    echo "  -c, --country-codes CODES Country codes to download data for (default: ${COUNTRY_CODES[*]})"
    echo "  -w, --workers N           Number of parallel download workers (default: $MAX_WORKERS)"
    echo "  -r, --retries N           Maximum number of retries for failed downloads (default: $MAX_RETRIES)"
    echo "  -o, --output-dir DIR      Output directory for downloaded files (default: $OUTPUT_DIR)"
    echo "  --timeout SECONDS         Processing timeout (default: ${TIMEOUT}s)"
    echo "  -t, --test                Run in test mode with a small subset of locations"
    echo "  -h, --help                Display this help message"
    echo ""
    echo "Configuration Integration:"
    echo "  This script uses the centralized configuration system when available."
    echo "  Configuration defaults are loaded from config/config.yaml."
    echo "  Command-line arguments override configuration defaults."
    echo ""
    echo "Configuration Status:"
    echo "  Config system: $([[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Available" || echo "❌ Using fallbacks")"
    echo "  Default country codes: ${COUNTRY_CODES[*]}"
    echo "  Default workers: $MAX_WORKERS"
    echo "  Default output dir: $OUTPUT_DIR"
    echo "  Default timeout: ${TIMEOUT}s"
    echo ""
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -y|--years)
            shift
            YEARS=()
            while [[ $# -gt 0 && ! "$1" =~ ^- ]]; do
                YEARS+=("$1")
                shift
            done
            ;;
        -c|--country-codes)
            shift
            COUNTRY_CODES=()
            while [[ $# -gt 0 && ! "$1" =~ ^- ]]; do
                COUNTRY_CODES+=("$1")
                shift
            done
            ;;
        -w|--workers)
            MAX_WORKERS="$2"
            shift 2
            ;;
        -r|--retries)
            MAX_RETRIES="$2"
            shift 2
            ;;
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        -t|--test)
            TEST_MODE=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            log "Unknown option: $1"
            usage
            ;;
    esac
done

# Make sure the output directory exists
mkdir -p "$OUTPUT_DIR"

# Format the years and country codes for the command
YEARS_STR=$(printf "%s " "${YEARS[@]}")
COUNTRY_CODES_STR=$(printf "%s " "${COUNTRY_CODES[@]}")

# Start the collection process
log "Starting historical OpenAQ data collection"
if [ "$TEST_MODE" = true ]; then
    log "Running in TEST MODE with a small subset of locations"
    log "Output directory: $OUTPUT_DIR"
    
    # Run the test script with limited parameters
    python3 src/data_processors/s3_historical_data.py \
        --years 2024 \
        --country-codes 111 \
        --max-workers 2 \
        --max-retries $MAX_RETRIES \
        --output-dir "$OUTPUT_DIR" 2>&1 | tee -a "$LOG_FILE"
else
    log "Years: ${YEARS_STR}"
    log "Country codes: ${COUNTRY_CODES_STR}"
    log "Maximum workers: $MAX_WORKERS"
    log "Output directory: $OUTPUT_DIR"
    
    # Run the Python script
    python3 src/data_processors/s3_historical_data.py \
        --years $YEARS_STR \
        --country-codes $COUNTRY_CODES_STR \
        --max-workers $MAX_WORKERS \
        --max-retries $MAX_RETRIES \
        --output-dir "$OUTPUT_DIR" 2>&1 | tee -a "$LOG_FILE"
fi

# Check if the script succeeded
EXIT_CODE=${PIPESTATUS[0]}
if [ $EXIT_CODE -eq 0 ]; then
    log "Historical data collection completed successfully"
else
    log "ERROR: Historical data collection failed with exit code $EXIT_CODE"
    exit $EXIT_CODE
fi

# Log completion
log "OpenAQ historical data collection process finished" 
