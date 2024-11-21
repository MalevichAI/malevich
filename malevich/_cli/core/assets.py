import os
import tempfile
import zipfile
from typing import Optional

import humanfriendly
import rich
from rich.progress import Progress
from rich.tree import Tree
from typer import Typer

from ..._utility.asset_full_tree import get_asset_full_tree
from malevich.core_api import (
    delete_collection_object,
    get_collection_object,
    get_collection_objects,
    update_collection_object,
)

from ..misc.make import wrap_command

assets_app = Typer(name='assets')


@assets_app.command(name='list')
@wrap_command(get_collection_objects)
def list_assets(
    sizes: bool = False,
    plain: bool = False,
    **kwargs
) -> None:
    kwargs.pop('path', None)
    file_dirs = get_asset_full_tree(
        base_path='/',
        conn_url=kwargs.get('conn_url'),
        auth=kwargs.get('auth'),
    )

    if plain:
        if sizes:
            length_ = max(map(len, file_dirs.files.keys()))
            contents = '\n'.join([
                f'{p.ljust(1 + length_)}\t{humanfriendly.format_size(size)}'
                for p, size in file_dirs.files.items()
            ])
        else:
            contents = '\n'.join(file_dirs.files.keys())
        print(contents)
        return

    tree = Tree('assets')
    for file, size in file_dirs.files.items():
        path = file.split('/')
        node = tree
        for p in path[:-1]:
            node = node.add(p)
        p = path[-1]
        if sizes:
            node.add(
                f'{p} [bright_black]({humanfriendly.format_size(size)})[/bright_black]'
            )
        else:
            node.add(p)
    rich.print()
    rich.print(tree)
    rich.print()


@assets_app.command(name='get')
@wrap_command(get_collection_object)
def get(save: Optional[str] = None, **kwargs) -> None:
    path = os.path.basename(kwargs.get('path'))
    if save:
        bytes_ = get_collection_object(**kwargs)
        if os.path.isdir(save):
            path = os.path.join(save, path)
        with open(path, 'wb') as f:
            f.write(bytes_)
    else:
        get_collection_objects(recursive=False, **kwargs)
        name, size = next(
            get_collection_objects(recursive=False, **kwargs).files.items()
        )
        rich.print(
            f'{name}\t[bright_black]{humanfriendly.format_size(size)}[/bright_black]')


@assets_app.command(name='upload')
@wrap_command(update_collection_object, exclude=['data', 'zip'])
def update(local_path: str, **kwargs) -> None:
    local_path = os.path.abspath(os.path.expanduser(local_path))
    if not os.path.exists(local_path):
        rich.print("\nPath is invalid")
        exit(-1)

    if os.path.isdir(local_path):

        with Progress() as progress:
            files = set().union(*[
                [os.path.join(r, f) for f in fs]
                for r, _, fs in os.walk(local_path)
            ])
            archive_task = progress.add_task(
                'Compressing files',
                total=len(files)
            )
            with tempfile.TemporaryFile('wb+') as tf, zipfile.ZipFile(tf, 'w') as f:
                for fl in files:
                    f.write(fl)
                    progress.update(archive_task, advance=1)
                f.close()
                tf.seek(0)
                bytes_ = tf.read()

                update_collection_object(
                    data=bytes_,
                    zip=True,
                    **kwargs,
                )
    else:
        update_collection_object(
            data=open(local_path, 'rb'),
            zip=False,
            **kwargs,
        )

@assets_app.command(name='delete')
@wrap_command(delete_collection_object)
def delete(**kwargs) -> None:
    delete_collection_object(**kwargs)

