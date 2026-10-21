@smoke
Feature: Setup Script Smoke
  Ensure setup can prepare the runtime environment successfully.

  Scenario: setup.sh completes with success markers
    When I run "./scripts/setup.sh" with timeout 180
    Then the command should succeed
    And the command output should contain "Runtime setup completed successfully!"
    And the command output should contain "System is ready for air quality prediction pipeline"
