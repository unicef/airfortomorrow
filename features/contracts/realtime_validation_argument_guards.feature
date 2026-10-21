@contract
Feature: Realtime Mode Invalid Validation Flag Guard
  Realtime prediction should reject historical validation options.

  Scenario: Realtime prediction rejects --validate-sensors
    When I run "./scripts/predict_air_quality.sh --mode realtime --countries THA LAO --validate-sensors" with timeout 120
    Then the command should fail
    And the command output should contain "validate-sensors is not a valid option for realtime mode"
