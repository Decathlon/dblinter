from unittest.mock import Mock

import pytest
from pydantic import ValidationError
from pydantic_yaml import parse_yaml_raw_as
from ruamel.yaml.scanner import ScannerError

from dblinter.configuration import Configuration
from dblinter.configuration_model import ConfigurationModel
from dblinter.function_library import FunctionLibrary


def test_filemane_not_exist() -> None:
    configuration = Configuration()
    with pytest.raises(IOError):
        configuration.read_config_yaml_file(
            path="tests/data", config_file_name="config_file_missing.yaml"
        )


def test_config_file_syntax_error() -> None:
    with pytest.raises(ScannerError):
        configuration = Configuration()
        raw_yaml = configuration.read_config_yaml_file(
            path="tests/data", config_file_name="config_file_syntax_error.yaml"
        )
        parse_yaml_raw_as(ConfigurationModel, raw_yaml)


def test_config_file_empty() -> None:
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file(
        path="tests/data", config_file_name="config_file_empty.yaml"
    )
    assert raw_yaml is not None
    assert raw_yaml == ""
    with pytest.raises(ValidationError):
        parse_yaml_raw_as(ConfigurationModel, raw_yaml)


def test_configuration_attribute_readable() -> None:
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file(
        path="dblinter", config_file_name="default_config.yaml"
    )
    configuration.config_file = parse_yaml_raw_as(ConfigurationModel, raw_yaml)
    assert configuration.config_file.cluster_checks.get_enabled_checks() is not None
    assert configuration.config_file.base_checks.get_enabled_checks() is not None
    assert configuration.config_file.table_checks.get_enabled_checks() is not None
    assert (
        configuration.config_file.cluster_checks.get_enabled_checks()[0].enabled is True
    )


def test_check_in_config_are_in_function_list() -> None:
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file(
        path="tests/data", config_file_name="config_file_wrong_function_name.yaml"
    )
    configuration.config_file = parse_yaml_raw_as(ConfigurationModel, raw_yaml)
    # config_file = Configuration()
    # config_file.read_config_file("tests/data", "config_file_wrong_function_name.yaml")
    function_library = FunctionLibrary()
    with pytest.raises(IOError):
        configuration.check_in_config_are_in_function_list(function_library)


def test_read_config_from_bucket(mocker) -> None:
    """Test Read config file from bucket.

    step:
    1 - call read_config
    result: should_pass
    """
    # Mock the client
    mock_client = mocker.patch("dblinter.configuration.Client")

    # Set up the mock bucket and blob
    mock_bucket = Mock()
    with open("tests/data/good_config.yaml", "r", encoding="utf-8") as file:
        conf = file.read()
    mock_bucket.blob.return_value.download_as_bytes.return_value = conf.encode("utf-8")
    mock_client().get_bucket.return_value = mock_bucket

    # Create Configuration instance
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file(
        "dblinter", "gs://bucketname/dblinter.cfg"
    )
    configuration.config_file = parse_yaml_raw_as(ConfigurationModel, raw_yaml)

    mock_client().get_bucket.assert_called_with("bucketname")
    mock_bucket.blob.assert_called_with("dblinter.cfg")


def test_read_config_from_bucket_not_exists(mocker) -> None:
    """Test Read config file from bucket but file does not exist.

    step:
    1 - call read_config
    result: should_pass
    """
    # Mock the client
    mock_client = mocker.patch("dblinter.configuration.Client")

    # Set up the mock bucket and blob
    mock_bucket = Mock()
    with open("tests/data/good_config.yaml", "r", encoding="utf-8") as file:
        conf = file.read()
    mock_bucket.blob.return_value.download_as_bytes.return_value = conf.encode("utf-8")
    mock_bucket.blob.return_value.exists.return_value = False
    mock_client().get_bucket.return_value = mock_bucket

    # Create Configuration instance
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file(
        "dblinter", "gs://bucketname/dblinter.cfg"
    )
    configuration.config_file = parse_yaml_raw_as(ConfigurationModel, raw_yaml)

    mock_client().get_bucket.assert_called_with("bucketname")
    mock_bucket.blob.assert_called_with("dblinter.cfg")
    assert not mock_bucket.blob().exists()
