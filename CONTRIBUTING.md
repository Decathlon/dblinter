## Installation

### Install poetry
curl -sSL https://install.python-poetry.org | python3 -

### Install dependencies
```sh
poetry install --with dev
```

### Launch the program
```sh
poetry run python -m dblinter
```

### Launch the tests
You need to have a started docker engine
```sh
poetry run pytest
```
---

## Using new package

if want to use a new package in the code, you need you add it with poetry

For a package in use with the main program 
```sh
poetry add <package_name>
```

For a package used only in dev like pytest or linters
```sh
poetry add <package_name> --group dev
```

You can specify version requirement with the [@](https://python-poetry.org/docs/dependency-specification/#using-the--operator) operator.


---
## Submit a change

Make a new branch named "[Fix/Feat/Doc]/issue_id/description-in-kebab-case" with your code and push it to the repo.

---
## Add a new rule

Rules are divided in 4 categories:

| Categories    | RuleId |
|---------------|--------|
| Cluster rules | Cxxx   |
| Base rules    | Bxxx   |
| Schema rules  | Sxxx   |
| Table rules   | Txxx   |

Take the next RuleId available in your rule category

### Describe your rule in [default_config.yaml](https://github.com/decathlon/dblinter/blob/main/dblinter/default_config.yaml)

In the matching category, put your rule description in the config file

- name: *Your function's name in CamelCase*
- ruleid: *Your ruleid*
- enabled: *[True/False] Control is the rule will be used*
- params: *Optional. If your rule need parameters, you can defined any parameter you need*
  - warning: 10%
  - error: 70%
  - threshold: 1000000
  - ....
- context:*Mandatory. Define characters string used in the sarif output*
  - desc: *A description of the rule*
  - message: *The message to output in the sarif file when the rule detect something. You can use {0,1,..} notation to use variable in the message* 
  - fixes: *List possible fix to this problem*

### Create the rule

Rules must be created in "rules/RuleId/FunctionName.py". Remember the function name must match the name you put in the config.yaml file.
This file contain only one function with the check.

### Test the rule

The tests for your function must be located in tests/rules/RuleId/test_FunctionName.py
Make any test needed to have a full coverage.
You can run a full coverage test with: 

```shell
poetry run python -m pytest --cov-report term-missing --cov=dblinter tests
```
