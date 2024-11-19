import logging
import os
import shutil
from typing import Optional

from google.cloud.storage.client import Client
from pydantic import BaseModel

from dblinter.configuration_model import ConfigurationModel

LOGGER = logging.getLogger("dblinter")


DEFAULT_CONFIG_FILE_NAME = "default_config.yaml"
CONFIG_FILE_CLUSTER_CATEGORY = "cluster"
CONFIG_FILE_BASE_CATEGORY = "base"
CONFIG_FILE_TABLE_CATEGORY = "table"
CONFIG_FILE_SCHEMA_CATEGORY = "schema"


class Configuration(BaseModel):
    """manage configuration file

    Attributes:
        clusters_checks (list) : rules in configuration file at the cluster level
        base_checks (list) : rules in configuration file at the database level
        table_checks (list) : rules in configuration file at the table level
        schema_checks (list) : rules in configuration file at the schema level

    Methods:
        add_if_enabled(config_file_check_type, check_list) : Add rule in the check array only if enabled in the configuration file
    """

    config_file: Optional[ConfigurationModel] = None

    def check_in_config_are_in_function_list(self, function_library):
        """Check if the rule in the configuration file exists

        Args:
            config_file (Configuration): A configuration object with a parsed configuration file
            function_library (FunctionLibrary): A FunctionLibrary object with every rules available

        Raises:
            OSError: A rule in the configuration file does not exist
        """
        all_function_in_config = (
            self.config_file.cluster_checks.get_enabled_checks()
            + self.config_file.base_checks.get_enabled_checks()
            + self.config_file.table_checks.get_enabled_checks()
            + self.config_file.schema_checks.get_enabled_checks()
        )
        for function_in_config in all_function_in_config:
            check_is_present = False
            for function_name_in_function_list in function_library.functions_list:
                if function_in_config.name == function_name_in_function_list[1]:
                    check_is_present = True
            if check_is_present is False:
                raise OSError(
                    "Function " + function_in_config.name + " does not exist "
                )

    def read_config_yaml_file(self, path: str, config_file_name: str) -> str:
        """Read a configuration file

        Args:
            path (str): Directory containing the config file
            config_file_name (str): Config file name

        Raises:
            file_not_found: Unable to open the file

        Returns:
            str: raw yaml as a string
        """
        raw_yaml = None
        # If the name begin with gs:// we expect the file is on a GCP bucket
        if "gs://" in config_file_name:
            bucket_name = config_file_name.split("/")[2]
            object_name = "/".join(config_file_name.split("/")[3:])
            bucket = Client().get_bucket(bucket_name)
            blob = bucket.blob(object_name)
            if not blob.exists():
                with open(
                    f"{path}/{DEFAULT_CONFIG_FILE_NAME}", "r", encoding="utf-8"
                ) as fd:
                    blob.upload_from_file(fd)
            raw_yaml = blob.download_as_bytes()
        # else it's a regular file
        else:
            if not os.path.exists(f"{path}/{config_file_name}"):
                shutil.copyfile(f"{path}/{DEFAULT_CONFIG_FILE_NAME}", config_file_name)
            try:
                with open(path + "/" + config_file_name, "r", encoding="utf-8") as fd:
                    raw_yaml = fd.read()
            except IOError as file_not_found:
                LOGGER.error("[%s] not found", config_file_name)
                raise file_not_found

        if raw_yaml is None:
            LOGGER.error("Empty configuration file")

        return raw_yaml
