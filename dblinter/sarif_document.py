import logging
from datetime import datetime, timezone

from jschema_to_python.to_json import to_json
from rich import print as rprint
from sarif_om import (
    ArtifactLocation,
    Invocation,
    Location,
    Message,
    PhysicalLocation,
    Result,
    Run,
    SarifLog,
    Tool,
    ToolComponent,
)

from dblinter import __version__

LOGGER = logging.getLogger("dblinter")

VERSION = "2.1.0"
SCHEMA = "https://schemastore.azurewebsites.net/schemas/json/sarif-2.1.0-rtm.5.json"
RICH_RULE_MESSAGE_COLOR = "red"
RICH_RULE_FIX_COLOR = "green"


class SarifDocument:
    """Construct the sarif document

    Attributes:
        sarif_doc: the sarif document, a sarif_om object

    Methods:
        add_run_information(host): Add run information to the sarif document
        add_check(ruleid, message_args, uri, context): Add check result in the sarif document
    """

    sarif_doc = SarifLog(runs=[], version=VERSION, schema_uri=SCHEMA)
    quiet_mode: bool = False

    def __init__(self, host=None):
        self.add_run_information(host)
        self.translationDict = {}

    def add_run_information(self, host):
        """Add run information to the sarif document

        Args:
            host (str): the database host
        """
        tool = Tool(
            driver=ToolComponent(
                name="dblinter",
                information_uri="https://github.com/decathlon/dblinter",
                version=__version__,
            )
        )
        invocation = []
        invocation.append(
            Invocation(
                machine=host,
                start_time_utc=datetime.now(timezone.utc),
                execution_successful=True,
            )
        )
        run = []
        run.append(Run(tool=tool, results=[], invocations=invocation))
        self.sarif_doc.runs = run

    def add_check(self, ruleid, message_args, uri, context):
        """Add a check result to the sarif document

        Args:
            ruleid (str): The rule ID
            message_args (str[]): str list to fill variables in the message
            uri (str): the object concerned by the rule
            context (context object): information specific to a rule to build the check result
        """
        location = []
        location.append(
            Location(
                physical_location=PhysicalLocation(
                    artifact_location=ArtifactLocation(uri=uri)
                )
            )
        )
        message = context.message
        formated_fixes = []
        if message is not None:
            message = message.format(*message_args)
        else:
            message = ""
        if context.fixes is not None:
            formated_fixes = [fix.format(*message_args) for fix in context.fixes]
        sarif_result = Result(
            rule_id=ruleid,
            message=Message(text=message, arguments=message_args),
            fixes=formated_fixes,
            locations=location,
        )
        self.sarif_doc.runs[0].results.append(sarif_result)
        self.sarif_doc.runs[0].invocations[0].end_time_utc = datetime.now(timezone.utc)

        if self.quiet_mode is False:
            rprint(
                "["
                + RICH_RULE_MESSAGE_COLOR
                + "]  ⚠ - "
                + ruleid
                + " "
                + uri
                + " "
                + message
                + "[/"
                + RICH_RULE_MESSAGE_COLOR
                + "]"
            )
            if context.fixes:
                for fix in formated_fixes:
                    rprint(
                        "["
                        + RICH_RULE_FIX_COLOR
                        + "]    ↪ Fix:  "
                        + fix
                        + "[/"
                        + RICH_RULE_FIX_COLOR
                        + "]"
                    )

    def json_format(self):
        """Tranform a sarif_om object into json

        Returns:
            str: json sarif document
        """
        return to_json(self.sarif_doc)
