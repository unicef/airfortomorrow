@contract
Feature: Behave harness defaults
  The default suite must run without live services or slow workflows.

  Scenario: The Docker test runner exposes its usage
    When I run "./scripts/run_behave_tests.sh --help" with timeout 60
    Then the command should succeed
    And the command output should contain "Run Behave BDD tests"

  Scenario Outline: Default test selection respects service and runtime tags
    Then default test selection for tags "<tags>" should be "<selected>"

    Examples:
      | tags      | selected |
      | contract  | yes      |
      | live      | no       |
      | slow      | no       |
      | slow live | no       |
