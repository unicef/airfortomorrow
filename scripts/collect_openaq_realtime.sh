#!/bin/bash

# OpenAQ Near Real-Time Data Collection Script
# This script collects near real-time air quality data from OpenAQ for Thailand and Laos
# It's designed to be run regularly via cron, e.g., every 6 hours

# Set the base directory to the project root (parent of scripts/)
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$BASE_DIR"

# Set up script directory for configuration access
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source configuration and utility functions
if [[ -f "$SCRIPT_DIR/utils/config_reader.sh" ]]; then
    source "$SCRIPT_DIR/utils/config_reader.sh"
    CONFIG_AVAILABLE=true
else
    CONFIG_AVAILABLE=false
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
LOG_FILE="$LOG_DIR/openaq_realtime_${TIMESTAMP}.log"

# Configuration-driven defaults with fallbacks
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    DEFAULT_DAYS=$(get_config_time_window_days "realtime")
    DEFAULT_TIMEOUT=$(get_config_timeout "download_timeout")
    [[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Using configuration system for defaults"
else
    # Fallback defaults if config system unavailable
    DEFAULT_DAYS=2
    DEFAULT_TIMEOUT=1800
    echo "⚠️  Configuration system unavailable, using hardcoded defaults"
fi

# Default values (config-driven with fallbacks)
DAYS="$DEFAULT_DAYS"
TIMEOUT="$DEFAULT_TIMEOUT"
TEST_MODE=false
LIMIT=""
DEFAULT_TEST_LIMIT="${AIR_QUALITY_TEST_OPENAQ_LIMIT:-5}"

if [[ "${AIR_QUALITY_TEST_MODE:-0}" == "1" ]]; then
    TEST_MODE=true
    LIMIT="$DEFAULT_TEST_LIMIT"
fi

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --days)
            DAYS="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --limit)
            LIMIT="$2"
            shift 2
            ;;
        --test-mode)
            TEST_MODE=true
            if [[ -z "$LIMIT" ]]; then
                LIMIT="$DEFAULT_TEST_LIMIT"
            fi
            shift
            ;;
        --help)
            echo "OpenAQ Real-time Data Collection Script (Configuration-Aware)"
            echo ""
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --days N           Number of days to collect (default: $DEFAULT_DAYS)"
            echo "  --timeout SECONDS  Processing timeout (default: ${DEFAULT_TIMEOUT}s)"
            echo "  --limit N          Limit number of locations processed"
            echo "  --test-mode        Enable bounded test mode (sets default --limit to $DEFAULT_TEST_LIMIT)"
            echo "  --help            Display this help message"
            echo ""
            echo "Configuration Integration:"
            echo "  This script uses the centralized configuration system when available."
            echo "  Configuration defaults are loaded from config/config.yaml."
            echo ""
            echo "Configuration Status:"
            echo "  Config system: $([[ "$CONFIG_AVAILABLE" == true ]] && echo "✅ Available" || echo "❌ Using fallbacks")"
            echo "  Default days: $DEFAULT_DAYS"
            echo "  Default timeout: ${DEFAULT_TIMEOUT}s"
            echo "  Test mode default location limit: $DEFAULT_TEST_LIMIT"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--days N] [--timeout SECONDS] [--limit N] [--test-mode] [--help]"
            exit 1
            ;;
    esac
done

echo "Starting OpenAQ real-time data collection..."
echo "Days to collect: $DAYS"
echo "Timeout: ${TIMEOUT}s"
echo "Test mode: $([ "$TEST_MODE" == true ] && echo "enabled" || echo "disabled")"
if [[ -n "$LIMIT" ]]; then
    echo "Location limit: $LIMIT"
fi
echo "Log file: $LOG_FILE"
[[ "$CONFIG_AVAILABLE" == true ]] && echo "Configuration system: ✅ Available" || echo "Configuration system: ❌ Using fallbacks"

# Run the Python script and capture output
echo "$(date): Starting data collection" >> "$LOG_FILE"

# Use the original sequential approach with openaq_realtime_client.py
PYTHON_CMD=(python -m src.data_processors.openaq_realtime_client --days "$DAYS")
if [[ -n "$LIMIT" ]]; then
    PYTHON_CMD+=(--limit "$LIMIT")
fi
if [[ "$TEST_MODE" == true ]]; then
    PYTHON_CMD+=(--test-mode)
fi

"${PYTHON_CMD[@]}" 2>&1 | tee -a "$LOG_FILE"

# Check if the script ran successfully
if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo "$(date): Data collection completed successfully" >> "$LOG_FILE"
    echo "OpenAQ real-time data collection completed successfully!"
else
    echo "$(date): Data collection failed with exit code ${PIPESTATUS[0]}" >> "$LOG_FILE"
    echo "OpenAQ real-time data collection failed!"
    exit 1
fi 
