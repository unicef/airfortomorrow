#!/usr/bin/env python3
"""
Complete Air Quality Data Pipeline Runner

Master script that orchestrates all data collection and processing pipelines:
- Himawari (satellite AOD data)
- FIRMS (fire detection data) 
- ERA5 (meteorological data)
- OpenAQ (air quality monitoring data)
- AirGradient (sensor data)

Usage Examples:
    # Run all pipelines in real-time mode (uses config realtime_hours: 48)
    python src/run_complete_pipeline.py --mode realtime --countries LAO THA
    
    # Run specific pipelines only
    python src/run_complete_pipeline.py --mode realtime --pipelines himawari era5 firms --countries LAO THA
    
    # Historical processing for research
    python src/run_complete_pipeline.py --mode historical --start-date 2025-06-01 --end-date 2025-06-03 --countries LAO THA
    
    # Real-time with custom settings (hours parameter ignored, uses config)
    python src/run_complete_pipeline.py --mode realtime --skip-openaq --skip-airgradient --countries THA
    
    # Parallel execution (faster)
    python src/run_complete_pipeline.py --mode realtime --parallel --countries LAO THA
"""

import sys
import os
import subprocess
import argparse
import logging
from datetime import datetime
from pathlib import Path
import threading
import queue
import time

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

# Import config loader
from src.utils.config_loader import ConfigLoader

class PipelineRunner:
    """Master pipeline runner for all air quality data collection and processing."""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent.absolute()
        self.script_dir = Path(__file__).parent.parent.absolute()
        self.log_dir = self.base_dir / "logs"
        self.log_dir.mkdir(exist_ok=True)
        
        # Initialize config loader
        self.config_loader = ConfigLoader()
        
        # Setup logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"complete_pipeline_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Pipeline configurations
        self.pipelines = {
            'himawari': {
                'name': 'Himawari Satellite AOD',
                'script': self.script_dir / 'scripts' / 'run_himawari_integrated_pipeline.sh',
                'realtime_cmd': self._build_himawari_realtime_cmd,
                'historical_cmd': self._build_himawari_historical_cmd,
                'supports_parallel': True
            },
            'firms': {
                'name': 'FIRMS Fire Detection',
                'script': self.script_dir / 'scripts' / 'run_firms_pipeline.sh',
                'realtime_cmd': self._build_firms_realtime_cmd,
                'historical_cmd': self._build_firms_historical_cmd,
                'supports_parallel': True
            },
            'era5': {
                'name': 'ERA5 Meteorological',
                'script': self.script_dir / 'scripts' / 'run_era5_idw_pipeline.sh',
                'realtime_cmd': self._build_era5_realtime_cmd,
                'historical_cmd': self._build_era5_historical_cmd,
                'supports_parallel': True
            },
            'openaq': {
                'name': 'OpenAQ Air Quality',
                'realtime_script': self.script_dir / 'scripts' / 'collect_openaq_realtime.sh',
                'historical_script': self.script_dir / 'scripts' / 'collect_openaq_historical.sh',
                'realtime_cmd': self._build_openaq_realtime_cmd,
                'historical_cmd': self._build_openaq_historical_cmd,
                'supports_parallel': True
            },
            'airgradient': {
                'name': 'AirGradient Sensors',
                'script': self.script_dir / 'scripts' / 'collect_airgradient_data.sh',
                'realtime_cmd': self._build_airgradient_realtime_cmd,
                'historical_cmd': self._build_airgradient_historical_cmd,
                'supports_parallel': True
            }
        }
        
        # Execution results
        self.results = {}
        self.start_time = None
        self.end_time = None

    def _build_himawari_realtime_cmd(self, args):
        """Build command for Himawari real-time processing."""
        return [
            str(self.pipelines['himawari']['script']),
            '--mode', 'realtime',
            '--hours', str(args.hours),
            '--countries'] + args.countries

    def _build_himawari_historical_cmd(self, args):
        """Build command for Himawari historical processing."""
        return [
            str(self.pipelines['himawari']['script']),
            '--mode', 'historical',
            '--start-date', args.start_date,
            '--end-date', args.end_date,
            '--countries'] + args.countries

    def _build_firms_realtime_cmd(self, args):
        """Build command for FIRMS real-time processing."""
        return [
            str(self.pipelines['firms']['script']),
            '--data-type', 'realtime',
            '--countries'] + args.countries

    def _build_firms_historical_cmd(self, args):
        """Build command for FIRMS historical processing."""
        return [
            str(self.pipelines['firms']['script']),
            '--data-type', 'historical',
            '--start-date', args.start_date,
            '--end-date', args.end_date,
            '--countries'] + args.countries

    def _build_era5_realtime_cmd(self, args):
        """Build command for ERA5 real-time processing."""
        return [
            str(self.pipelines['era5']['script']),
            '--mode', 'realtime', 
            '--hours', str(args.hours),
            '--countries'] + args.countries

    def _build_era5_historical_cmd(self, args):
        """Build command for ERA5 historical processing."""
        return [
            str(self.pipelines['era5']['script']),
            '--mode', 'historical',
            '--start-date', args.start_date,
            '--end-date', args.end_date,
            '--countries'] + args.countries

    def _build_openaq_realtime_cmd(self, args):
        """Build command for OpenAQ real-time processing."""
        # OpenAQ scripts are hardcoded for Thailand/Laos, but we can pass days
        cmd = [
            str(self.pipelines['openaq']['realtime_script']),
            '--days', str(max(1, args.hours // 24))]
        if getattr(args, 'test_mode', False):
            cmd.extend([
                '--test-mode',
                '--limit', str(args.test_openaq_limit),
            ])
        return cmd

    def _build_openaq_historical_cmd(self, args):
        """Build command for OpenAQ historical processing."""
        # Extract years from date range for OpenAQ historical script
        start_year = args.start_date.split('-')[0]
        end_year = args.end_date.split('-')[0]
        
        cmd = [
            str(self.pipelines['openaq']['historical_script']),
            '--years', start_year]
        
        # Add end year if different from start year
        if start_year != end_year:
            cmd.append(end_year)
        if getattr(args, 'test_mode', False):
            cmd.append('--test')
            
        return cmd

    def _build_airgradient_realtime_cmd(self, args):
        """Build command for AirGradient real-time processing."""
        # Use config value for realtime hours instead of command line args
        config_hours = self.config_loader.get_time_window('realtime')
        # AirGradient script supports mode and days
        return [
            str(self.pipelines['airgradient']['script']),
            '--mode', 'realtime',
            '--days', str(max(1, config_hours // 24))]

    def _build_airgradient_historical_cmd(self, args):
        """Build command for AirGradient historical processing."""
        return [
            str(self.pipelines['airgradient']['script']),
            '--mode', 'historical',
            '--start-date', args.start_date,
            '--end-date', args.end_date]

    def _build_air_quality_processing_cmd(self, args):
        """Build command for air quality data processing."""
        cmd = [
            str(self.script_dir / 'scripts' / 'process_air_quality.sh'),
            '--mode', args.mode,
            '--hours', str(args.hours),
            '--countries'] + args.countries
        
        if args.mode == 'historical':
            cmd.extend(['--start-date', args.start_date, '--end-date', args.end_date])
        
        return cmd

    def validate_args(self, args):
        """Validate command line arguments."""
        self.logger.info("Validating arguments...")
        
        # Validate mode
        if args.mode not in ['realtime', 'historical']:
            raise ValueError(f"Invalid mode: {args.mode}. Must be 'realtime' or 'historical'")
        
        # Validate mode-specific parameters
        if args.mode == 'historical':
            if not args.start_date or not args.end_date:
                raise ValueError("Historical mode requires both --start-date and --end-date")
            
            # Parse and validate dates
            try:
                start = datetime.strptime(args.start_date, '%Y-%m-%d')
                end = datetime.strptime(args.end_date, '%Y-%m-%d')
                if start > end:
                    raise ValueError("Start date must be before end date")
            except ValueError as e:
                raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {e}")
                
        elif args.mode == 'realtime':
            if args.hours < 1 or args.hours > 168:
                raise ValueError("Hours must be between 1 and 168 (1 week)")
        
        # Validate countries
        valid_countries = {'LAO', 'THA', 'VNM', 'KHM', 'IDN', 'MYS', 'SGP', 'BRN'}
        for country in args.countries:
            if country not in valid_countries:
                self.logger.warning(f"Country code '{country}' may not be supported. "
                                  f"Valid codes: {', '.join(sorted(valid_countries))}")
        
        # Validate pipelines
        available_pipelines = set(self.pipelines.keys())
        if args.pipelines:
            for pipeline in args.pipelines:
                if pipeline not in available_pipelines:
                    raise ValueError(f"Invalid pipeline: {pipeline}. "
                                   f"Available: {', '.join(sorted(available_pipelines))}")
        
        self.logger.info("Arguments validated successfully")

    def get_pipelines_to_run(self, args):
        """Determine which pipelines to run based on arguments."""
        if args.pipelines:
            # Specific pipelines requested
            pipelines_to_run = args.pipelines
        else:
            # All pipelines by default, but check skip flags
            pipelines_to_run = list(self.pipelines.keys())
            
            if args.skip_himawari:
                pipelines_to_run.remove('himawari')
            if args.skip_firms:
                pipelines_to_run.remove('firms')
            if args.skip_era5:
                pipelines_to_run.remove('era5')
            if args.skip_openaq:
                pipelines_to_run.remove('openaq')
            if args.skip_airgradient:
                pipelines_to_run.remove('airgradient')
        
        return pipelines_to_run

    def run_pipeline(self, pipeline_name, args, result_queue=None):
        """Run a single pipeline."""
        try:
            pipeline_config = self.pipelines[pipeline_name]
            pipeline_display_name = pipeline_config['name']
            
            self.logger.info(f"Starting {pipeline_display_name} pipeline...")
            start_time = time.time()
            
            # Build command based on mode
            if args.mode == 'realtime':
                cmd = pipeline_config['realtime_cmd'](args)
            else:
                cmd = pipeline_config['historical_cmd'](args)
            
            # Add additional arguments
            if hasattr(args, 'verbose') and args.verbose:
                if pipeline_name not in ['himawari', 'firms']:
                    cmd.append('--verbose')
            
            # Execute command
            self.logger.info(f"Executing: {' '.join(cmd)}")
            env = os.environ.copy()
            if getattr(args, 'test_mode', False):
                env['AIR_QUALITY_TEST_MODE'] = '1'
                env['AIR_QUALITY_TEST_OPENAQ_LIMIT'] = str(args.test_openaq_limit)
            result = subprocess.run(
                cmd,
                cwd=self.script_dir,
                capture_output=True,
                text=True,
                timeout=args.timeout if hasattr(args, 'timeout') else 3600,
                env=env,
            )
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Process results
            success = result.returncode == 0
            
            pipeline_result = {
                'name': pipeline_display_name,
                'success': success,
                'duration': duration,
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'command': ' '.join(cmd)
            }
            
            if success:
                self.logger.info(f"✅ {pipeline_display_name} completed successfully in {duration:.1f}s")
                
                # Run air quality processing after OpenAQ and AirGradient
                if pipeline_name == 'airgradient' and 'openaq' in self.results and self.results['openaq']['success']:
                    self.logger.info("Running air quality processing step...")
                    aq_cmd = self._build_air_quality_processing_cmd(args)
                    aq_result = subprocess.run(
                        aq_cmd,
                        cwd=self.script_dir,
                        capture_output=True,
                        text=True,
                        timeout=args.timeout if hasattr(args, 'timeout') else 3600
                    )
                    if aq_result.returncode == 0:
                        self.logger.info("✅ Air quality processing completed successfully")
                    else:
                        self.logger.error(f"❌ Air quality processing failed: {aq_result.stderr}")
            else:
                self.logger.error(f"❌ {pipeline_display_name} failed after {duration:.1f}s")
                self.logger.error(f"Error output: {result.stderr}")
            
            # Store result
            if result_queue:
                result_queue.put((pipeline_name, pipeline_result))
            else:
                self.results[pipeline_name] = pipeline_result
                
        except subprocess.TimeoutExpired:
            self.logger.error(f"❌ {pipeline_display_name} timed out")
            pipeline_result = {
                'name': pipeline_display_name,
                'success': False,
                'duration': args.timeout if hasattr(args, 'timeout') else 3600,
                'returncode': -1,
                'stdout': '',
                'stderr': 'Pipeline timed out',
                'command': ' '.join(cmd) if 'cmd' in locals() else 'Unknown'
            }
            
            if result_queue:
                result_queue.put((pipeline_name, pipeline_result))
            else:
                self.results[pipeline_name] = pipeline_result
                
        except Exception as e:
            self.logger.error(f"❌ {pipeline_display_name} failed with exception: {e}")
            pipeline_result = {
                'name': pipeline_display_name,
                'success': False,
                'duration': 0,
                'returncode': -1,
                'stdout': '',
                'stderr': str(e),
                'command': 'Unknown'
            }
            
            if result_queue:
                result_queue.put((pipeline_name, pipeline_result))
            else:
                self.results[pipeline_name] = pipeline_result

    def run_pipelines_sequential(self, pipelines_to_run, args):
        """Run pipelines sequentially."""
        self.logger.info(f"Running {len(pipelines_to_run)} pipelines sequentially...")
        
        for pipeline_name in pipelines_to_run:
            self.run_pipeline(pipeline_name, args)
            
            # Check if we should stop on failure
            if hasattr(args, 'stop_on_failure') and args.stop_on_failure:
                if not self.results[pipeline_name]['success']:
                    self.logger.error(f"Stopping pipeline execution due to failure in {pipeline_name}")
                    break

    def run_pipelines_parallel(self, pipelines_to_run, args):
        """Run pipelines in parallel."""
        self.logger.info(f"Running {len(pipelines_to_run)} pipelines in parallel...")
        
        # Create result queue for thread communication
        result_queue = queue.Queue()
        threads = []
        
        # Start threads
        for pipeline_name in pipelines_to_run:
            thread = threading.Thread(
                target=self.run_pipeline,
                args=(pipeline_name, args, result_queue)
            )
            thread.daemon = True
            thread.start()
            threads.append((pipeline_name, thread))
            self.logger.info(f"Started {pipeline_name} thread")
        
        # Wait for all threads and collect results
        completed = 0
        while completed < len(pipelines_to_run):
            try:
                pipeline_name, result = result_queue.get(timeout=10)
                self.results[pipeline_name] = result
                completed += 1
                self.logger.info(f"Progress: {completed}/{len(pipelines_to_run)} pipelines completed")
            except queue.Empty:
                # Check if any threads are still alive
                alive_threads = [name for name, thread in threads if thread.is_alive()]
                if alive_threads:
                    self.logger.info(f"Still running: {', '.join(alive_threads)}")
                else:
                    break
        
        # Ensure all threads complete
        for pipeline_name, thread in threads:
            thread.join(timeout=60)
            if thread.is_alive():
                self.logger.warning(f"Thread {pipeline_name} did not complete gracefully")

    def print_results_summary(self, args):
        """Print summary of pipeline execution results."""
        self.logger.info("\n" + "="*80)
        self.logger.info("PIPELINE EXECUTION SUMMARY")
        self.logger.info("="*80)
        
        # Overall statistics
        total_duration = self.end_time - self.start_time
        successful = sum(1 for r in self.results.values() if r['success'])
        total = len(self.results)
        
        self.logger.info(f"Mode: {args.mode.upper()}")
        self.logger.info(f"Countries: {', '.join(args.countries)}")
        if args.mode == 'realtime':
            self.logger.info(f"Time window: {args.hours} hours")
        else:
            self.logger.info(f"Date range: {args.start_date} to {args.end_date}")
        self.logger.info(f"Execution mode: {'Parallel' if args.parallel else 'Sequential'}")
        self.logger.info(f"Total duration: {total_duration:.1f} seconds")
        self.logger.info(f"Success rate: {successful}/{total} ({100*successful/total:.1f}%)")
        
        # Individual pipeline results
        self.logger.info("\nPipeline Results:")
        self.logger.info("-" * 80)
        
        for pipeline_name, result in self.results.items():
            status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
            duration = result['duration']
            self.logger.info(f"{result['name']:<25} {status:<10} {duration:>8.1f}s")
            
            if not result['success'] and result['stderr']:
                # Show first few lines of error
                error_lines = result['stderr'].split('\n')[:3]
                for line in error_lines:
                    if line.strip():
                        self.logger.info(f"    Error: {line.strip()}")
        
        # Data output locations
        self.logger.info("\nData Output Locations:")
        self.logger.info("-" * 80)
        output_paths = {
            'himawari': 'data/processed/himawari/',
            'firms': 'data/processed/firms/',
            'era5': 'data/processed/era5/daily_aggregated/',
            'openaq': 'data/raw/openaq/',
            'airgradient': 'data/raw/airgradient/'
        }
        
        for pipeline_name in self.results.keys():
            if pipeline_name in output_paths:
                path = self.script_dir / output_paths[pipeline_name]
                if path.exists():
                    try:
                        # Get directory size
                        total_size = sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
                        size_mb = total_size / (1024 * 1024)
                        file_count = len(list(path.rglob('*')))
                        self.logger.info(f"{pipeline_name:<15} {str(path):<50} {size_mb:>8.1f} MB ({file_count} files)")
                    except:
                        self.logger.info(f"{pipeline_name:<15} {str(path):<50} (size unknown)")
                else:
                    self.logger.info(f"{pipeline_name:<15} {str(path):<50} (not found)")
        
        self.logger.info("="*80)

    def run(self, args):
        """Run the complete pipeline."""
        try:
            self.start_time = time.time()
            
            # Log execution start
            self.logger.info("🚀 Starting Complete Air Quality Data Pipeline")
            self.logger.info(f"Mode: {args.mode}")
            self.logger.info(f"Countries: {', '.join(args.countries)}")
            
            if args.mode == 'realtime':
                self.logger.info(f"Time window: {args.hours} hours lookback")
            else:
                self.logger.info(f"Date range: {args.start_date} to {args.end_date}")
            
            # Validate arguments
            self.validate_args(args)
            
            # Determine pipelines to run
            pipelines_to_run = self.get_pipelines_to_run(args)
            self.logger.info(f"Pipelines to run: {', '.join(pipelines_to_run)}")
            
            if not pipelines_to_run:
                self.logger.warning("No pipelines selected to run!")
                return 0
            
            # Run pipelines
            if args.parallel and len(pipelines_to_run) > 1:
                self.run_pipelines_parallel(pipelines_to_run, args)
            else:
                self.run_pipelines_sequential(pipelines_to_run, args)
            
            self.end_time = time.time()
            
            # Print results summary
            self.print_results_summary(args)
            
            # Return exit code based on results
            failed_pipelines = [name for name, result in self.results.items() if not result['success']]
            if failed_pipelines:
                self.logger.error(f"❌ {len(failed_pipelines)} pipeline(s) failed: {', '.join(failed_pipelines)}")
                return 1
            else:
                self.logger.info("✅ All pipelines completed successfully!")
                return 0
                
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
            return 1

def main():
    parser = argparse.ArgumentParser(
        description='Complete Air Quality Data Pipeline Runner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Mode and timing
    parser.add_argument('--mode', choices=['realtime', 'historical'], default='realtime',
                       help='Processing mode (default: realtime)')
    parser.add_argument('--hours', type=int, default=24,
                       help='Hours to look back in realtime mode (default: 24)')
    parser.add_argument('--start-date', type=str,
                       help='Start date for historical mode (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                       help='End date for historical mode (YYYY-MM-DD)')
    
    # Pipeline selection
    parser.add_argument('--pipelines', nargs='+', 
                       choices=['himawari', 'firms', 'era5', 'openaq', 'airgradient'],
                       help='Specific pipelines to run (default: all)')
    parser.add_argument('--skip-himawari', action='store_true',
                       help='Skip Himawari satellite AOD pipeline')
    parser.add_argument('--skip-firms', action='store_true',
                       help='Skip FIRMS fire detection pipeline')
    parser.add_argument('--skip-era5', action='store_true',
                       help='Skip ERA5 meteorological pipeline')
    parser.add_argument('--skip-openaq', action='store_true',
                       help='Skip OpenAQ air quality pipeline')
    parser.add_argument('--skip-airgradient', action='store_true',
                       help='Skip AirGradient sensor pipeline')
    
    # Geographic settings
    parser.add_argument('--countries', nargs='+', default=['LAO', 'THA'],
                       help='Country codes to process (default: LAO THA)')
    
    # Execution options
    parser.add_argument('--parallel', action='store_true',
                       help='Run pipelines in parallel (faster but more resource intensive)')
    parser.add_argument('--stop-on-failure', action='store_true',
                       help='Stop execution if any pipeline fails (sequential mode only)')
    parser.add_argument('--timeout', type=int, default=3600,
                       help='Timeout for individual pipelines in seconds (default: 3600)')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose output for pipelines that support it')
    parser.add_argument('--test-mode', action='store_true',
                       help='Enable bounded test mode for collectors')
    parser.add_argument('--test-openaq-limit', type=int, default=5,
                       help='OpenAQ location cap used with --test-mode (default: 5)')
    
    args = parser.parse_args()
    
    # Run the complete pipeline
    runner = PipelineRunner()
    return runner.run(args)

if __name__ == "__main__":
    sys.exit(main()) 
