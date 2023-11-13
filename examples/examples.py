from typing import Annotated

import pandas as pd
from examples.taxonomy import flows
import typer
import rich
from malevich.interpreter.space import SpaceInterpreter

app = typer.Typer()


@app.command('upload')
def upload(reverse_id: Annotated[str, typer.Argument()]):
    flow = [x for x in flows if x[0].reverse_id == reverse_id]
    if not flow:
        rich.print(f"[red]There is no flow with[/red] reverse_id={reverse_id}")

    flow = flow[0]
    flow_args = flow[1]
    for k in flow_args.keys():
        flow_args[k] = pd.DataFrame(flow_args[k])
 

    task = flow[0](**flow_args)
    task.interpret(SpaceInterpreter())



if __name__ == '__main__':
    app()

