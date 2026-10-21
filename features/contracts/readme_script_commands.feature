@contract
  Feature: README Script Command Contract
  README script commands should expose a usable help interface.

  Scenario Outline: Script command help returns usage text without unknown option errors
    When I run "<command>" with timeout 45
    Then the command exit code should be one of "0,1"
    And the command output should not contain "Unknown option"
    And the command output should contain "usage" case-insensitively

    Examples:
      | command |
      | ./scripts/run_complete_pipeline.sh --mode realtime --countries THA LAO --generate-maps --parallel --help |
      | ./scripts/run_complete_pipeline.sh --mode realtime --countries THA LAO --generate-maps --help |
      | ./scripts/run_complete_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO --generate-maps --help |
      | ./scripts/run_air_quality_integrated_pipeline.sh --mode realtime --countries THA LAO --help |
      | ./scripts/run_air_quality_integrated_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-30 --countries THA LAO --help |
      | ./scripts/run_firms_pipeline.sh --mode realtime --countries THA LAO --help |
      | ./scripts/run_firms_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-30 --countries THA LAO --help |
      | ./scripts/run_himawari_integrated_pipeline.sh --mode realtime --countries THA LAO --help |
      | ./scripts/run_himawari_integrated_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-30 --countries THA LAO --help |
      | ./scripts/run_era5_idw_pipeline.sh --mode realtime --countries THA LAO --help |
      | ./scripts/run_era5_idw_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-30 --countries THA LAO --help |
      | ./scripts/make_silver.sh --mode realtime --countries THA LAO --help |
      | ./scripts/make_silver.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-30 --countries THA LAO --help |
      | ./scripts/predict_air_quality.sh --mode realtime --countries THA LAO --generate-map --help |
      | ./scripts/predict_air_quality.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO --generate-maps --validate-sensors --enhanced-maps --help |
      | ./scripts/run_tests.sh --help |
      | ./scripts/smoke_test.sh --help |
