@era5 @contract
Feature: ERA5 realtime collector contract
  Protect ERA5 realtime CLI and output-data contracts used by pipeline wrappers.

  Scenario: run_era5_idw_pipeline script exposes expected CLI options
    When I run "./scripts/run_era5_idw_pipeline.sh --help" with timeout 60
    Then the command should succeed
    And the command output should include ERA5 CLI options "--mode,--hours,--countries,--params,--idw-rings,--idw-weight-power,--timeout,--force,--help"

  Scenario: ERA5 integrated Python entrypoint exposes expected CLI options
    When I run "python -m src.era5_integrated_pipeline_idw --help" with timeout 60
    Then the command should succeed
    And the command output should include ERA5 CLI options "--mode,--start-date,--end-date,--hours,--params,--countries,--idw-rings,--idw-weight-power,--output-dir,--raw-data-dir,--force,--log-level"

  @live @benchmark
  Scenario: ERA5-only realtime run produces fresh, useful parquet output
    Given I clean the "data" directory while preserving git-tracked files
    When I run "./scripts/run_era5_idw_pipeline.sh --mode realtime --hours 24 --countries THA --params 2t --idw-rings 8 --idw-weight-power 1.5 --force --timeout 2400" with timeout 2600
    Then the command should succeed
    And the command output should contain "ERA5 IDW Pipeline completed successfully!"
    And the command output should reference an existing log file
    And the current run should produce at least 1 updated ERA5 realtime parquet files
    And the latest updated ERA5 realtime parquet should contain at least 1 rows
    And the latest updated ERA5 realtime parquet should include columns "h3_08,date"
    And the latest updated ERA5 realtime parquet should include at least one meteorological column from "2t,t2m,temperature_2m"
    And the latest updated ERA5 realtime parquet should have non-null values in columns "h3_08,date"
    And the latest updated ERA5 realtime parquet should have numeric values in the selected meteorological column
    And the latest updated ERA5 realtime parquet should have at least 100 unique values in column "h3_08"
    And the referenced log file should not contain "Waiting 10s before next request to avoid rate limiting..."
