#!/bin/bash

# Complete Air Quality Data Pipeline Runner (Shell Wrapper)
# This script runs the complete air quality data pipeline in sequence:
# 1. Data Collection (Himawari, FIRMS, ERA5, OpenAQ, AirGradient)
# 2. Silver Dataset Generation (combines all data sources)
# 3. Air Quality Prediction with optional Map Generation

# Set the base directory to the project root (parent of scripts/)
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$BASE_DIR"

# Set up script directory for configuration access
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source configuration and utility functions
CONFIG_READER="$SCRIPT_DIR/utils/config_reader.sh"
CONFIG_FILE="$BASE_DIR/config/config.yaml"

if [[ -f "$CONFIG_READER" ]]; then
    source "$CONFIG_READER"
    CONFIG_AVAILABLE=true
    
    # Get configuration values
    DEFAULT_COUNTRIES=($(get_config_countries))
    DEFAULT_H3_RESOLUTION=$(get_config_h3_resolution)
    DEFAULT_MAP_RESOLUTION=$(get_config_h3_resolution)
    DEFAULT_HOURS=24
    DEFAULT_TIMEOUT=$(get_config_timeout "pipeline_default" "3600")
    DEFAULT_MAX_WORKERS=64
    
    log_info() { echo -e "${BLUE}ℹ️  [$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}" | tee -a "$LOG_FILE"; }
    log_success() { echo -e "${GREEN}✅ [$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}" | tee -a "$LOG_FILE"; }
    log_warning() { echo -e "${YELLOW}⚠️  [$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}" | tee -a "$LOG_FILE"; }
    log_error() { echo -e "${RED}❌ [$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}" | tee -a "$LOG_FILE"; }
    
    echo "✅ $(date '+%Y-%m-%d %H:%M:%S') Using configuration system for defaults"
else
    CONFIG_AVAILABLE=false
    
    # Fallback defaults
    DEFAULT_COUNTRIES=("LAO" "THA")
    DEFAULT_H3_RESOLUTION=8
    DEFAULT_MAP_RESOLUTION=6
    DEFAULT_HOURS=24
    DEFAULT_TIMEOUT=3600
    DEFAULT_MAX_WORKERS=64
    
    echo "⚠️  $(date '+%Y-%m-%d %H:%M:%S') Configuration system not available, using fallbacks"
    # Define basic logging if config system not available
    log_info() { echo "[INFO] $1" | tee -a "$LOG_FILE"; }
    log_success() { echo "[SUCCESS] $1" | tee -a "$LOG_FILE"; }
    log_warning() { echo "[WARN] $1" | tee -a "$LOG_FILE"; }
    log_error() { echo "[ERROR] $1" | tee -a "$LOG_FILE"; }
fi

# Create logs directory if it doesn't exist
if [[ "$CONFIG_AVAILABLE" == true ]]; then
    LOG_DIR=$(get_config_path "logs" "./logs")
else
    LOG_DIR="$BASE_DIR/logs"
fi
mkdir -p "$LOG_DIR"

# Set log file with timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/complete_pipeline_${TIMESTAMP}.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Function to log messages with colors
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log_step() {
    echo -e "${CYAN}🔄 [$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}" | tee -a "$LOG_FILE"
}

log_pipeline() {
    echo -e "${PURPLE}🚀 [$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}" | tee -a "$LOG_FILE"
}

# Initialize script variables
MODE="realtime"
COUNTRIES=("${DEFAULT_COUNTRIES[@]}")
HOURS=24
START_DATE=""
END_DATE=""
PARALLEL=false
GENERATE_MAPS=true
SKIP_SILVER=false
SKIP_PREDICTION=false
MAP_RESOLUTION="$DEFAULT_MAP_RESOLUTION"

# Default settings (config-driven with fallbacks)
MODE="realtime"
HOURS="$DEFAULT_HOURS"
COUNTRIES=("${DEFAULT_COUNTRIES[@]}")
PARALLEL=false

# New pipeline control flags
SKIP_SILVER=false
SKIP_PREDICTION=false
GENERATE_MAPS=true
MAP_RESOLUTION="$DEFAULT_MAP_RESOLUTION"
TIMEOUT="$DEFAULT_TIMEOUT"
MAX_WORKERS="$DEFAULT_MAX_WORKERS"
TEST_MODE=false
TEST_OPENAQ_LIMIT=5

# Sensor validation flags (historical mode defaults)
VALIDATE_SENSORS=true
ENHANCED_MAPS=true
SAVE_VALIDATION=true

# Help message
show_help() {
    cat << EOF
Air Quality Prediction Complete Pipeline

USAGE:
    $0 [OPTIONS]

DESCRIPTION:
    Runs the complete air quality prediction pipeline including data collection,
    processing, silver dataset generation, and prediction.

OPTIONS:
    -m, --mode MODE       Pipeline mode: 'realtime' or 'historical' (default: realtime)
    -c, --countries LIST  Space-separated list of countries (default: from config)
    -h, --hours HOURS     Hours to look back for realtime mode (default: 24)
    -s, --start-date DATE Start date for historical mode (YYYY-MM-DD)
    -e, --end-date DATE   End date for historical mode (YYYY-MM-DD)
    -p, --parallel        Run data collection in parallel (experimental)
    --test-mode           Enable bounded test mode for collectors
    --test-openaq-limit N OpenAQ location cap when --test-mode is enabled (default: 5)
    --generate-maps       Generate prediction maps
    --map-resolution RES  H3 resolution for maps (default: from config)
    --skip-silver         Skip silver dataset generation
    --skip-prediction     Skip prediction phase
    --no-sensor-validation  Disable sensor validation (historical mode default: enabled)
    --no-enhanced-maps      Disable enhanced maps with sensor overlays (historical mode default: enabled)
    --no-save-validation    Disable saving validation results (historical mode default: enabled)
    --help                Show this help message

EXAMPLES:
    # Run realtime pipeline for last 24 hours
    $0 --mode realtime --hours 24

    # Run historical pipeline for date range
    $0 --mode historical --start-date 2024-01-01 --end-date 2024-01-31

    # Run with maps generation
    $0 --mode realtime --generate-maps

    # Run only data collection and processing (skip prediction)
    $0 --mode realtime --skip-prediction
    
    # Run historical mode without sensor validation (validation is enabled by default)
    $0 --mode historical --start-date 2024-01-01 --end-date 2024-01-31 --no-sensor-validation --no-enhanced-maps

NOTES:
    Sensor validation options are historical-mode only. In realtime mode,
    --validate-sensors, --enhanced-maps, and --save-validation are not applied.

CONFIGURATION:
$(if [[ "$CONFIG_AVAILABLE" == true ]]; then
    echo "  Configuration file: Available ($CONFIG_FILE)"
    echo "  Default countries: ${DEFAULT_COUNTRIES[*]}"
    echo "  Default H3 resolution: $DEFAULT_H3_RESOLUTION"
else
    echo "  Configuration file: Not available (using fallbacks)"
    echo "  Default countries: ${DEFAULT_COUNTRIES[*]} (fallback)"
    echo "  Default H3 resolution: $DEFAULT_H3_RESOLUTION (fallback)"
fi)

FILES:
    Log file: $LOG_FILE
    Configuration: $CONFIG_FILE

EOF
}

# Parse command line arguments and build Python command
PYTHON_ARGS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            MODE="$2"
            PYTHON_ARGS="$PYTHON_ARGS --mode $2"
            shift 2
            ;;
        --hours)
            HOURS="$2"
            PYTHON_ARGS="$PYTHON_ARGS --hours $2"
            shift 2
            ;;
        --start-date)
            START_DATE="$2"
            PYTHON_ARGS="$PYTHON_ARGS --start-date $2"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
            PYTHON_ARGS="$PYTHON_ARGS --end-date $2"
            shift 2
            ;;
        --pipelines)
            PYTHON_ARGS="$PYTHON_ARGS --pipelines"
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                PYTHON_ARGS="$PYTHON_ARGS $1"
                shift
            done
            ;;
        --countries)
            shift
            COUNTRIES=()
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                COUNTRIES+=("$1")
                shift
            done
            # Build countries argument for Python script
            if [ ${#COUNTRIES[@]} -gt 0 ]; then
                PYTHON_ARGS="$PYTHON_ARGS --countries ${COUNTRIES[*]}"
            fi
            ;;
        --skip-himawari)
            PYTHON_ARGS="$PYTHON_ARGS --skip-himawari"
            shift
            ;;
        --skip-firms)
            PYTHON_ARGS="$PYTHON_ARGS --skip-firms"
            shift
            ;;
        --skip-era5)
            PYTHON_ARGS="$PYTHON_ARGS --skip-era5"
            shift
            ;;
        --skip-openaq)
            PYTHON_ARGS="$PYTHON_ARGS --skip-openaq"
            shift
            ;;
        --skip-airgradient)
            PYTHON_ARGS="$PYTHON_ARGS --skip-airgradient"
            shift
            ;;
        --skip-silver)
            SKIP_SILVER=true
            shift
            ;;
        --skip-prediction)
            SKIP_PREDICTION=true
            shift
            ;;
        --generate-maps)
            GENERATE_MAPS=true
            shift
            ;;
        --map-resolution)
            MAP_RESOLUTION="$2"
            shift 2
            ;;
        --no-sensor-validation)
            VALIDATE_SENSORS=false
            shift
            ;;
        --no-enhanced-maps)
            ENHANCED_MAPS=false
            shift
            ;;
        --no-save-validation)
            SAVE_VALIDATION=false
            shift
            ;;
        --parallel)
            PARALLEL=true
            PYTHON_ARGS="$PYTHON_ARGS --parallel"
            shift
            ;;
        --test-mode)
            TEST_MODE=true
            PYTHON_ARGS="$PYTHON_ARGS --test-mode"
            shift
            ;;
        --test-openaq-limit)
            TEST_OPENAQ_LIMIT="$2"
            PYTHON_ARGS="$PYTHON_ARGS --test-openaq-limit $2"
            shift 2
            ;;
        --verbose)
            PYTHON_ARGS="$PYTHON_ARGS --verbose"
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Sensor validation features are historical-only.
if [[ "$MODE" == "realtime" ]]; then
    if [[ "$VALIDATE_SENSORS" == true || "$ENHANCED_MAPS" == true || "$SAVE_VALIDATION" == true ]]; then
        log_info "Realtime mode detected: disabling historical-only sensor validation options"
    fi
    VALIDATE_SENSORS=false
    ENHANCED_MAPS=false
    SAVE_VALIDATION=false
fi

# Record pipeline start time
PIPELINE_START_TIME=$(date +%s)

# Display configuration
echo "=========================================" | tee -a "$LOG_FILE"
log_pipeline "COMPLETE AIR QUALITY PIPELINE STARTING"
echo "=========================================" | tee -a "$LOG_FILE"
log "Configuration:"
log "  Mode: $MODE"
log "  Countries: ${COUNTRIES[*]}"
if [ "$MODE" = "realtime" ]; then
    log "  Time window: $HOURS hours"
else
    log "  Date range: $START_DATE to $END_DATE"
fi
if [ "$PARALLEL" = true ]; then
    log "  Execution mode: Parallel"
else
    log "  Execution mode: Sequential"
fi
log "  Generate maps: $([ "$GENERATE_MAPS" = true ] && echo "Yes (resolution $MAP_RESOLUTION)" || echo "No")"
log "  Test mode: $([ "$TEST_MODE" = true ] && echo "ENABLED (OpenAQ limit: $TEST_OPENAQ_LIMIT)" || echo "DISABLED")"
if [[ "$MODE" == "historical" ]]; then
    log "  Sensor validation: $([ "$VALIDATE_SENSORS" = true ] && echo "ENABLED" || echo "DISABLED")"
    log "  Enhanced maps: $([ "$ENHANCED_MAPS" = true ] && echo "ENABLED" || echo "DISABLED")"
    log "  Save validation: $([ "$SAVE_VALIDATION" = true ] && echo "ENABLED" || echo "DISABLED")"
else
    log "  Sensor validation: N/A (historical mode only)"
    log "  Enhanced maps: N/A (historical mode only)"
    log "  Save validation: N/A (historical mode only)"
fi
echo "" | tee -a "$LOG_FILE"

log "Pipeline steps:"
log "  Data Collection: ENABLED"
log "  Air Quality Data Processing: $([ "$SKIP_SILVER" = true ] && echo "SKIPPED" || echo "ENABLED")"
log "  Silver Dataset: $([ "$SKIP_SILVER" = true ] && echo "SKIPPED" || echo "ENABLED")"
log "  Prediction: $([ "$SKIP_PREDICTION" = true ] && echo "SKIPPED" || echo "ENABLED")"
echo "" | tee -a "$LOG_FILE"

# Function to run a step with error handling
run_step() {
    local step_name="$1"
    local command="$2"
    local optional="${3:-false}"
    
    log_step "Starting: $step_name"
    local start_time=$(date +%s)
    
    # Run command directly and preserve the producer exit code (not tee's).
    bash -c "$command" 2>&1 | tee -a "$LOG_FILE"
    local exit_code=${PIPESTATUS[0]}
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    if [[ $exit_code -eq 0 ]]; then
        log_success "$step_name completed successfully (${duration}s)"
        return 0
    fi

    if [[ "$optional" == "true" ]]; then
        log_warning "$step_name failed but continuing (${duration}s, exit code: $exit_code)"
        return 0
    else
        log_error "$step_name failed after ${duration}s (exit code: $exit_code)"
    fi

    return $exit_code
}

# PHASE 1: Data Collection
log_pipeline "PHASE 1: DATA COLLECTION"
echo "" | tee -a "$LOG_FILE"

# Make pip install non-interactive and add error handling


log_info "Executing data collection pipelines..."
log_info "Command: python3 src/run_complete_pipeline.py $PYTHON_ARGS"

# Execute data collection pipeline and preserve producer exit code.
python3 src/run_complete_pipeline.py $PYTHON_ARGS 2>&1 | tee -a "$LOG_FILE"
exit_code=${PIPESTATUS[0]}
if [[ $exit_code -eq 0 ]]; then
    log_success "Data collection pipeline completed successfully"
else
    log_error "Data collection pipeline failed with exit code: $exit_code"
    exit $exit_code
fi

log_success "Data collection phase completed"

# PHASE 1.5: Air Quality Data Processing
if [[ "$SKIP_SILVER" != true ]]; then
    echo "" | tee -a "$LOG_FILE"
    log_pipeline "PHASE 1.5: AIR QUALITY DATA PROCESSING"
    echo "" | tee -a "$LOG_FILE"
    
    # Build air quality processing command
    AQ_CMD="./scripts/process_air_quality.sh --mode $MODE --countries ${COUNTRIES[*]}"
    
    if [ "$MODE" = "realtime" ]; then
        AQ_CMD="$AQ_CMD --hours $HOURS"
    else
        AQ_CMD="$AQ_CMD --start-date $START_DATE --end-date $END_DATE"
    fi
    
    run_step "Air Quality Data Processing" "$AQ_CMD"
    
    log_success "Air quality data processing phase completed"
else
    log_info "Skipping air quality data processing"
fi

# PHASE 2: Silver Dataset Generation
if [[ "$SKIP_SILVER" != true ]]; then
    echo "" | tee -a "$LOG_FILE"
    log_pipeline "PHASE 2: SILVER DATASET GENERATION"
    echo "" | tee -a "$LOG_FILE"
    
    # Build silver dataset command
    SILVER_CMD="./scripts/make_silver.sh --mode $MODE --countries ${COUNTRIES[*]}"
    
    if [ "$MODE" = "realtime" ]; then
        SILVER_CMD="$SILVER_CMD --hours $HOURS"
    else
        SILVER_CMD="$SILVER_CMD --start-date $START_DATE --end-date $END_DATE"
    fi
    
    run_step "Silver Dataset Generation" "$SILVER_CMD"
    
    log_success "Silver dataset generation phase completed"
else
    log_info "Skipping silver dataset generation"
fi

# PHASE 3: Air Quality Prediction
if [[ "$SKIP_PREDICTION" != true ]]; then
    echo "" | tee -a "$LOG_FILE"
    log_pipeline "PHASE 3: AIR QUALITY PREDICTION"
    echo "" | tee -a "$LOG_FILE"
    
    # Build prediction command
    PREDICT_CMD="./scripts/predict_air_quality.sh --mode $MODE --countries ${COUNTRIES[*]}"
    
    if [ "$MODE" = "historical" ]; then
        PREDICT_CMD="$PREDICT_CMD --start-date $START_DATE --end-date $END_DATE"
    fi
    
    if [[ "$GENERATE_MAPS" == true ]]; then
        PREDICT_CMD="$PREDICT_CMD --generate-map --map-resolution $MAP_RESOLUTION"
    fi
    
    # Add sensor validation options (enabled by default)
    if [[ "$VALIDATE_SENSORS" == true ]]; then
        PREDICT_CMD="$PREDICT_CMD --validate-sensors"
    fi
    
    if [[ "$ENHANCED_MAPS" == true ]]; then
        PREDICT_CMD="$PREDICT_CMD --enhanced-maps"
    fi
    
    if [[ "$SAVE_VALIDATION" == true ]]; then
        PREDICT_CMD="$PREDICT_CMD --save-validation"
    fi
    
    run_step "Air Quality Prediction" "$PREDICT_CMD"
    
    log_success "Air quality prediction phase completed"
else
    log_info "Skipping air quality prediction"
fi

# PIPELINE SUMMARY
echo "" | tee -a "$LOG_FILE"
log_pipeline "PIPELINE SUMMARY"
echo "=========================================" | tee -a "$LOG_FILE"

end_time=$(date +%s)
total_duration=$((end_time - PIPELINE_START_TIME))
hours=$((total_duration / 3600))
minutes=$(((total_duration % 3600) / 60))
seconds=$((total_duration % 60))

log "Total execution time: ${hours}h ${minutes}m ${seconds}s"
log "Countries processed: ${COUNTRIES[*]}"
if [ "$MODE" = "realtime" ]; then
    log "Time window: $HOURS hours"
else
    log "Date range: $START_DATE to $END_DATE"
fi
log "Execution mode: $([ "$PARALLEL" = true ] && echo "Parallel" || echo "Sequential")"
log "Maps generated: $([ "$GENERATE_MAPS" = true ] && echo "Yes" || echo "No")"
echo "" | tee -a "$LOG_FILE"

# Check output files
log_info "Output Summary:"

# Data collection outputs
DATA_DIRS=("./data/raw" "./data/processed")
for dir in "${DATA_DIRS[@]}"; do
    if [[ -d "$dir" ]]; then
        data_size=$(du -sh "$dir" 2>/dev/null | cut -f1 || echo "Unknown")
        log "  $dir: $data_size"
    fi
done

# Silver dataset
if [[ "$SKIP_SILVER" != true ]]; then
    if [[ "$MODE" == "realtime" ]]; then
        SILVER_DIR="./data/silver/realtime"
    else
        SILVER_DIR="./data/silver/historical"
    fi
    
    if [[ -d "$SILVER_DIR" ]]; then
        silver_files=$(find "$SILVER_DIR" -name "*.parquet" -newermt '-1 hour' 2>/dev/null | wc -l)
        log "  Silver datasets: $silver_files files"
    fi
fi

# Predictions
if [[ "$SKIP_PREDICTION" != true ]]; then
    if [[ "$MODE" == "realtime" ]]; then
        PRED_DIR="./data/predictions/data/realtime"
        MAP_DIR="./data/predictions/map/realtime"
    else
        PRED_DIR="./data/predictions/data/historical"
        MAP_DIR="./data/predictions/map/historical"
    fi
    
    if [[ -d "$PRED_DIR" ]]; then
        pred_files=$(find "$PRED_DIR" -name "*.parquet" -newermt '-1 hour' 2>/dev/null | wc -l)
        log "  Prediction files: $pred_files files"
    fi
    
    if [[ "$GENERATE_MAPS" == true && -d "$MAP_DIR" ]]; then
        map_files=$(find "$MAP_DIR" -name "*.png" -newermt '-1 hour' 2>/dev/null | wc -l)
        log "  AQI maps: $map_files files"
    fi
    
    # Check validation outputs
    if [[ "$VALIDATE_SENSORS" == true || "$ENHANCED_MAPS" == true || "$SAVE_VALIDATION" == true ]]; then
        VALIDATION_DIR="./data/predictions/validation"
        if [[ -d "$VALIDATION_DIR" ]]; then
            # Check validation data files (parquet and JSON)
            validation_data_files=$(find "$VALIDATION_DIR/data" -name "*.parquet" -newermt '-1 hour' 2>/dev/null | wc -l)
            validation_metrics=$(find "$VALIDATION_DIR/data" -name "*.json" -newermt '-1 hour' 2>/dev/null | wc -l)
            # Check validation maps
            validation_maps=$(find "$VALIDATION_DIR/map" -name "*.png" -newermt '-1 hour' 2>/dev/null | wc -l)
            # Check scatter plots
            scatter_plots=$(find "$VALIDATION_DIR/scatter" -name "*.png" -newermt '-1 hour' 2>/dev/null | wc -l)
            
            log "  Validation data: $validation_data_files files"
            log "  Validation metrics: $validation_metrics files"
            log "  Enhanced maps: $validation_maps files"
            log "  Scatter plots: $scatter_plots files"
        fi
    fi
fi

echo "" | tee -a "$LOG_FILE"
log "Log file: $LOG_FILE"

log_success "🎉 COMPLETE PIPELINE EXECUTION FINISHED SUCCESSFULLY!"
exit 0 
