"""The configuration file model

Contain the definition of data model for parsing the configuration file.
In our case, we use
basemodel, so this module contains Pydantic schema.
"""

# pylint: disable=no-name-in-module
# pylint: disable=no-self-argument

from __future__ import annotations

from typing import List, Optional, Union

from pydantic import BaseModel, Field, RootModel


class Context(BaseModel):
    """Model for a context in the configuration file

    Attributes:
        desc (str): The description of the context
        message (str): The message of the context
        fixes (list): The fixes of the context
    """

    desc: Optional[str] = None
    message: Optional[str] = None
    fixes: Optional[List[str]] = None


class Rule(BaseModel):
    """Model for a rule in the configuration file

    Attributes:
        name (str): The name of the rule
        ruleid (str): The rule ID
        enabled (bool): The status of the rule
        params (list): The parameters of the rule
        context (list): The context of the rule
    """

    name: str
    ruleid: str = None
    enabled: bool = None
    params: Optional[List[Union[dict[str, str], dict[str, int]]]] = None
    context: Context


class Rules(RootModel):
    """Model for a list of rule in the configuration file"""

    root: Optional[List[Rule]]

    def get_enabled_checks(self) -> List[Rule]:
        """Get all enabled rules

        Returns:
            List[Rule]: List of enabled rules
        """
        return [rule for rule in self.root if rule.enabled]


class ConfigurationModel(BaseModel):
    """Model for the yaml configuration file

    Attributes:
        clusters_checks (list) : rules in configuration file at the cluster level
        base_checks (list) : rules in configuration file at the database level
        table_checks (list) : rules in configuration file at the table level
        schema_checks (list) : rules in configuration file at the schema level
    """

    cluster_checks: Optional[Rules] = Field(alias="cluster")
    base_checks: Optional[Rules] = Field(alias="base")
    table_checks: Optional[Rules] = Field(alias="table")
    schema_checks: Optional[Rules] = Field(alias="schema")
