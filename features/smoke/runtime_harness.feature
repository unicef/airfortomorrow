@smoke
Feature: Runtime Harness Smoke
  Validate that the Docker BDD harness has required runtime properties.

  Scenario: Docker test environment marker is set
    Given the environment variable "AIR_QUALITY_TEST_IN_DOCKER" should equal "1"

  Scenario Outline: Core Python dependencies import successfully
    Given python module "<module_name>" should import successfully

    Examples:
      | module_name |
      | osgeo.gdal  |
      | eccodes     |

  Scenario Outline: Core dependency attributes are available
    Given python attribute "<attribute_path>" should exist

    Examples:
      | attribute_path               |
      | osgeo.gdal.__version__       |
      | eccodes.codes_get_api_version |
