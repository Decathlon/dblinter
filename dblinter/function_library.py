import importlib
import logging
import os
import sys
from inspect import getmembers, isfunction, stack
from typing import List, Union

LOGGER = logging.getLogger("dblinter")
RULES_DIRECTORY = "rules"

EXCLUDED_SCHEMAS = [
    "pg_toast",
    "pg_catalog",
    "information_schema",
    "_timescaledb_catalog",
    "_timescaledb_config",
    "_timescaledb_internal",
    "_timescaledb_cache",
    "_timescaledb_functions",
    "timescaledb",
    "pgaudit",
]
EXCLUDED_SCHEMAS_STR = "', '".join(EXCLUDED_SCHEMAS)


def extract_param(param: List[dict[str, Union[str, int]]], param_name: str) -> str:
    """Given a param list, return the parameter named param_name

    Args:
        param (List): parameters list as present in the config file
        param_name (str): parameter name

    Returns:
        str: Parameter value
    """
    param_value = None
    if param is not None:
        for x in param:
            if x.get(param_name) is not None:
                param_value = x.get(param_name)
    else:
        LOGGER.error("Error: no params in config file")
    return param_value


class FunctionLibrary:
    """Store the rules available

    The rules are stored in the rules directory
    The rule ID is a unique identifier with a letter identifying the scope and 3 digit; the first leter correspond to the scope:
        - C: a Cluster check
        - B: a Database check
        - S: a Schema check
        - T: a Table check
    This class scan the rule directories to fill the functions_list with references to python function for each rule

    Attributes:
        functions_list(List) : List all the functions available and reference:
            rule (str) : RuleID
            module_name (str) : the name in the configuration and ${file}.py in the rule directory
            function_name (str) : the python function name
            function (FunctionType) : reference to the function

    Methods:
        get_function_by_config_name(config_name) : get a reference to a function in the function library matching the name present in the config file
        get_function_by_function_name(function_name) : get a reference to a function in the function library matching the python function name in the rule directory
        get_ruleid_from_function_name(): examine the call stack to find the called function name and return the coresponding ruleid in the function list
    """

    def __init__(self, path=None):
        # scan the rules directory
        if path is None:
            path = ["dblinter"]
        elif isinstance(path, str):
            path = [path]

        self.functions_list = []

        for base_path in path:
            if not os.path.exists(f"{base_path}/{RULES_DIRECTORY}"):
                raise OSError(
                    f"Rules directory {RULES_DIRECTORY} does not exist in {base_path}"
                )
            self._scan_rules_directory(base_path)

    def _scan_rules_directory(self, path: str):
        for rule in os.listdir(f"{path}/{RULES_DIRECTORY}"):
            py_file_detected = 0
            for file in os.listdir(f"{path}/{RULES_DIRECTORY}/{rule}"):
                if file.endswith(".py"):
                    py_file_detected = py_file_detected + 1
                    if py_file_detected > 1:
                        raise OSError(
                            f"There can be only one .py file in folder : {path}/{RULES_DIRECTORY}/"
                            + rule
                        )

                    module_name, _ = file.split(".")

                    # import the module
                    spec = importlib.util.spec_from_file_location(
                        module_name, f"{path}/{RULES_DIRECTORY}/{rule}/{file}"
                    )
                    module = importlib.util.module_from_spec(spec)
                    sys.modules[module_name] = module
                    spec.loader.exec_module(module)

                    # search our function among all the members
                    function_name = ""
                    function = ""
                    for _function_name, function in getmembers(module, isfunction):
                        if function.__module__ == module.__name__:
                            function_name = _function_name
                    # Add the function to the function list
                    self.functions_list.append(
                        (rule, module_name, function_name, function)
                    )

    def get_function_by_config_name(self, config_name: str):
        """get a reference to a function in the function library matching the name present in the config file

        Args:
            config_name (str): function name as present in the config file

        Returns:
            FunctionType: a reference to a function in the library
        """
        for _, cn, _, f in self.functions_list:
            if config_name == cn:
                return f
        # Nothing was found
        return ""

    def get_function_by_function_name(self, function_name: str):
        """get a reference to a function in the function library matching the python function name in the rule directory
        Args:
            function_name (str): python function name
        Returns:
            FunctionType: a reference to a function in the library
        """
        for _, _, fn, f in self.functions_list:
            if function_name == fn:
                return f
        # Nothing was found
        return ""

    def get_ruleid_from_function_name(self):
        """examine the stack to find the called function name and return the corresponding ruleid in the function list

        Returns:
            str: the ruleid of the function we are in
        """
        # examine the stack to find the called function name
        # stack()[1][3]
        for rule, _, fn, _ in self.functions_list:
            if stack()[1][3] == fn:
                return rule
        # Nothing was found
        return ""
