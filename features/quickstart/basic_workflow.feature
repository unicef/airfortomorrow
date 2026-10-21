@quickstart
Feature: Quick Start Workflow
  Validate the core README quickstart workflow in atomic BDD scenarios.

  Scenario: README contains the documented first-run command path
    Given the file "README.md" should exist
    Then the file "README.md" should contain "./scripts/setup.sh"
    And the file "README.md" should contain "./scripts/run_complete_pipeline.sh --mode realtime --countries THA LAO --generate-maps --parallel"

  Scenario: env_template can be copied into a valid .env file in a clean workspace
    Given I create a temporary quickstart workspace
    When I copy "env_template" to ".env" in the temporary quickstart workspace
    Then the path ".env" in the temporary quickstart workspace should be a regular file
    And the temporary quickstart ".env" file should contain required credential keys

  Scenario: complete pipeline wrapper executes in atomic mode and writes a log
    When I run "./scripts/run_complete_pipeline.sh --mode realtime --countries THA LAO --skip-himawari --skip-firms --skip-era5 --skip-openaq --skip-airgradient --skip-silver --skip-prediction --test-mode --test-openaq-limit 3" with timeout 120
    Then the command should succeed
    And the command output should contain "COMPLETE AIR QUALITY PIPELINE STARTING"
    And the command output should contain "Sensor validation: N/A (historical mode only)"
    And the command output should contain "No pipelines selected to run!"
    And the command output should contain "COMPLETE PIPELINE EXECUTION FINISHED SUCCESSFULLY!"
    And the command output should reference an existing log file

  @contract
  Scenario: realtime run reports missing credentials through component logs
    Given I clean the "data" directory while preserving git-tracked files
    When I run "env OPENAQ_API_KEY= HIMAWARI_FTP_USER= HIMAWARI_FTP_PASSWORD= ./scripts/run_complete_pipeline.sh --mode realtime --countries THA LAO --skip-firms --skip-era5 --skip-airgradient --skip-silver --skip-prediction --parallel --test-mode --test-openaq-limit 3" with timeout 120
    Then the command exit code should be 1
    And the current run should generate OpenAQ and Himawari component logs
    And the current run logs should contain "OpenAQ API key not found"
    And the current run logs should contain "FTP username and password must be provided"

  @slow @live
  Scenario: full realtime quickstart command produces core output artifacts
    Given I clean the "data" directory while preserving git-tracked files
    When I run "./scripts/run_complete_pipeline.sh --mode realtime --countries THA LAO --generate-maps --parallel" with timeout 4200
    Then the command should succeed
    And the command output should reference an existing log file
    And the directory "data/silver/realtime" should contain at least 1 files matching "*.parquet" updated by the current command
    And the directory "data/predictions/data/realtime" should contain at least 1 files matching "*.parquet" updated by the current command
    And the directory "data/predictions/map/realtime" should contain at least 1 files matching "*.png" updated by the current command
    And the referenced log file should not contain unexpected error markers
