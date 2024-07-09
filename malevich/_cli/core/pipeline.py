import rich
from malevich_coretools import delete_pipeline, get_pipeline, get_pipelines
from typer import Typer

from ..misc.make import wrap_command

pipeline_app = Typer(name='pipeline')

@pipeline_app.command(name='get')
@wrap_command(get_pipeline)
def pipeline_get(**kwargs)-> None:
    pipeline = get_pipeline(**kwargs)
    rich.print(pipeline)

@pipeline_app.command(name='get-all')
@wrap_command(get_pipelines)
def pipelines_get(**kwargs)-> None:
    pipelines = get_pipelines(**kwargs)
    for pipe in pipelines:
        rich.print(pipe)

@pipeline_app.command(name='delete')
@wrap_command(delete_pipeline)
def pipeline_delete(**kwargs)-> None:
    delete_pipeline(**kwargs)
    rich.print("Pipeline has been deleted")
