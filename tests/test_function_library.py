import os

import pytest

from dblinter.function_library import FunctionLibrary, extract_param


def test_function_list_1() -> None:
    func_lib = FunctionLibrary()
    assert func_lib.functions_list[0] is not None


def test_extract_param_Ok() -> None:
    # Test extract_param with valid param
    param = [{"warning": "10%"}, {"error": "70%"}]
    assert extract_param(param, "error") == "70%"


def test_extract_param_NOK() -> None:
    param = [{"error": "70%"}]
    assert extract_param(param, "warning") is None


def test_extract_param_missing(caplog) -> None:
    param = None
    extract_param(param, "warning")
    assert caplog.record_tuples[0][2] == "Error: no params in config file"


# def test_get_function_name_by_config_name() -> None:
#     function_library = Function_library()
#     assert (
#         function_library.get_function_name_by_config_name(
#             "MaxConnectionsByWorkMemIsNotLargerThanMemory"
#         )
#         == "max_connection_by_workmem_is_not_larger_than_memory"
#     )


# def test_get_function_name_by_config_name_not_exists() -> None:
#     function_library = Function_library()
#     assert function_library.get_function_name_by_config_name("notexists") is None


def test_more_than_one_py_file_in_rule_folder(tmp_path, monkeypatch) -> None:
    dblinter_dir = tmp_path / "dblinter"
    dblinter_dir.mkdir()
    rule_dir = dblinter_dir / "rules"
    rule_dir.mkdir()
    rule = rule_dir / "B001"
    rule.mkdir()

    file1 = rule / "HowManyTableWithoutPrimaryKey.py"
    os.system("cp dblinter/rules/B001/HowManyTableWithoutPrimaryKey.py " + str(file1))

    file2 = rule / "HowManyRedudantIndex.py"
    os.system("cp dblinter/rules/B002/HowManyRedudantIndex.py " + str(file2))

    monkeypatch.chdir(tmp_path)
    with pytest.raises(OSError):
        FunctionLibrary()
