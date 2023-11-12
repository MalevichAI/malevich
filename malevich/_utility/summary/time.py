import json

import pandas as pd
import rich
from malevich_coretools import logs_clickhouse
from rich.table import Table

from ..._utility.host import fix_host
from ..._utility.summary.abstract import AbstractSummary
from ...interpreter.core import CoreInterpreterState


class CoreTimeSummary(AbstractSummary[CoreInterpreterState]):
    def __init__(self, interpreter_state: CoreInterpreterState) -> None:
        super().__init__(None, interpreter_state)

    def json(self) -> None:
        core_ids = self._interpreter_state.core_ops.values()

        logs = logs_clickhouse(
            auth=tuple(self._interpreter_state.params["core_auth"]),
            conn_url=fix_host(self._interpreter_state.params["core_host"])
        )

        parsed = json.loads(logs)
        df = pd.DataFrame(parsed['TaskAppRunTable'])
        df = df[df.app_id.isin(core_ids)]
        df.insert(
            len(df.columns),
            column="duration",
            # Duration of the operation in milliseconds
            value=(
                pd.to_datetime(df.timestamp_end) - \
                pd.to_datetime(df.timestamp_start)
            ).astype("timedelta64[s]").astype(int),
        )

        df.set_index("app_id", inplace=True)

        return df.duration.to_json(orient="index")

    def display(self) -> None:
        summary = json.loads(self.json())
        table = Table(title="Time Summary (Core, ms)")
        table.add_column("App", justify="left", style="cyan")
        table.add_column("Duration, s", justify="right", style="magenta")
        for app_id, duration in summary.items():
            op_name = str(app_id).split("-")[1]
            table.add_row(op_name, str(duration))

        rich.print(table)
