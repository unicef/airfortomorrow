@contract
Feature: Historical Mode Required Argument Guards
  Historical mode scripts should fail fast when start/end dates are missing.

  Scenario Outline: Historical command fails without required dates
    When I run "<command>" with timeout 45
    Then the command should fail
    And the command output should contain "<error_text>"

    Examples:
      | command | error_text |
      | ./scripts/run_air_quality_integrated_pipeline.sh --mode historical --countries THA LAO | Historical mode requires both --start-date and --end-date |
      | ./scripts/run_himawari_integrated_pipeline.sh --mode historical --countries THA LAO | Historical mode requires both --start-date and --end-date |
      | ./scripts/run_era5_idw_pipeline.sh --mode historical --countries THA LAO | Historical mode requires --start-date and --end-date |
      | ./scripts/make_silver.sh --mode historical --countries THA LAO | Historical mode requires --start-date and --end-date |
