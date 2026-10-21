@regression @prediction
Feature: Realtime Prediction Command Integrity
  Validate fail-fast argument behavior for realtime prediction mode.

  Scenario: realtime prediction rejects historical validation flags
    When I run "./scripts/predict_air_quality.sh --mode realtime --countries THA LAO --generate-map --validate-sensors --enhanced-maps --save-validation" with timeout 900
    Then the command should fail
    And the command output should contain "validate-sensors is not a valid option for realtime mode"
    And the command output should not contain "'NoneType' object has no attribute 'empty'"
