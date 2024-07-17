
import typing
from typing import *

import malevich.annotations
from malevich.models import ConfigArgument
from malevich._meta.decor import proc
from malevich._utility import Registry
from malevich.models.nodes import OperationNode
from .scheme import *

Registry().register("3c6db207cf2f4281a2209d18db7ac238", {'operation_id': '3c6db207cf2f4281a2209d18db7ac238', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'add_column', 'processor_id': 'add_column', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=AddColumn)
def add_column(
    df: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    column: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    value: Annotated["typing.Any", ConfigArgument(required=False)] = None,
    position: Annotated["typing.Optional[int]", ConfigArgument(required=False)] = None,
    skip_if_exists: Annotated["typing.Optional[bool]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["AddColumn"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Inserts a new column into a dataframe.

    ## Input:
        An arbitrary dataframe and context information
        with the following fields in the app config:
            column: name of the new column
            value: value of the new column
            position: position of the new column

    ## Output:
        The input dataframe with the new column inserted at the specified position.

    ## Details:
        The function takes in a dataframe as an input and adds a new column
        at the specified position. The new column has a constant value provided
        by the user in the application configuration.

        If the position is negative, the new column will be inserted from the
        end of the dataframe. For example, a position of -1 will insert the
        new column as the last column in the dataframe.

    ## Configuration:
        - column: str, default 'new_column'.
            The name of the new column.
        - value: any, default 'new_value'.
            The value of the new column.
        - position: int, default 0.
            The position to insert the new column. If positive, the new column will be inserted from the beginning of the dataframe. If negative, the new column will be inserted from the end of the dataframe.
        - skip_if_exists: bool, default False.
            If columns exists, no exception will be thrown if this flag is set.
    -----

    Args:
        df: The input dataframe.
        context: The context information.

    Returns:
        The dataframe with new column.
    """

    return OperationNode(
        operation_id="3c6db207cf2f4281a2209d18db7ac238",
        config=config,
        processor_id="add_column",
        package_id="utility",
        alias=alias,
    )

Registry().register("827d367d8f674d809bba4b035e12c49c", {'operation_id': '827d367d8f674d809bba4b035e12c49c', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'add_index', 'processor_id': 'add_index', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=None)
def add_index(
    df: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    alias: Optional["str"] = None,
    config: Optional[dict] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """
    Add index column to the dataframe

    ## Input:

    Any dataframe

    ## Output:

    Input dataframe with additional column `index`

    -----
    Args:
        df (DF[Any]): Any dataframe
    Returns:
        Input dataframe with additional column `index`
    """

    return OperationNode(
        operation_id="827d367d8f674d809bba4b035e12c49c",
        config=config,
        processor_id="add_index",
        package_id="utility",
        alias=alias,
    )

Registry().register("899494416ee74091994976d02672c7dc", {'operation_id': '899494416ee74091994976d02672c7dc', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'collect_asset', 'processor_id': 'collect_asset', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=None)
def collect_asset(
    df: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    alias: Optional["str"] = None,
    config: Optional[dict] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """
    Moves assets to shared filesystem

    ## Input:
        An asset

    ## Output:
        A dataframe with a column:
        - `path` (str): A path (or paths) to asset files in the shared FS.

    -----
    """

    return OperationNode(
        operation_id="899494416ee74091994976d02672c7dc",
        config=config,
        processor_id="collect_asset",
        package_id="utility",
        alias=alias,
    )

Registry().register("815b2c9ecf1c4adbbd2468f0f3016bdf", {'operation_id': '815b2c9ecf1c4adbbd2468f0f3016bdf', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'concat', 'processor_id': 'concat', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=True, config_model=None)
def concat(
    *input: Any, 
    alias: Optional["str"] = None,
    config: Optional[dict] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """
    Concat DataFrames into one.

    ## Input:

    DataFrames you want to concat.

    ## Output:

    Concatenated DataFrame.

    -----
    Args:
        input (Sink): DataFrames you want to concat.
    Return:
        Concatenated DataFrame
    """

    return OperationNode(
        operation_id="815b2c9ecf1c4adbbd2468f0f3016bdf",
        config=config,
        processor_id="concat",
        package_id="utility",
        alias=alias,
    )

Registry().register("9d6248a482f84a649035f1d4dd84ef81", {'operation_id': '9d6248a482f84a649035f1d4dd84ef81', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'download', 'processor_id': 'download', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=Download)
def download(
    links: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    prefix: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["Download"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Download files from the internet

    ## Input:
        A dataframe with a column:
        - `link` (str): links to files to download.

    ## Output:
        A dataframe with a column:
        - `file` (str): containing paths to downloaded files.

    ## Configuration:
        - `prefix`: str, default ''.
        A prefix to add to the paths of downloaded files. If not specified, files will be downloaded to the root of the app directory.

    -----

    Args:
        links (DF[Links]): Dataframe with links to download
        context (Context): Context object

    Returns:
        Dataframe with paths to downloaded files
    """

    return OperationNode(
        operation_id="9d6248a482f84a649035f1d4dd84ef81",
        config=config,
        processor_id="download",
        package_id="utility",
        alias=alias,
    )

Registry().register("fbbf20fa595c41b9bd7b11cb62a1ed1b", {'operation_id': 'fbbf20fa595c41b9bd7b11cb62a1ed1b', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'get_links_to_files', 'processor_id': 'get_links_to_files', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=GetLinksToFiles)
def get_links_to_files(
    df: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    expiration: Annotated["typing.Optional[int]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["GetLinksToFiles"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Get links to files produced during the workflow execution.

    ## Input:
        An arbitrary dataframe.

    ## Output:
        The same dataframe, but with all file paths replaced
        with openable links to the files.

    ## Configuration:
        - `expiration`: int, default 21600.
        The number of seconds after which the link will expire. Defaults to 6 hours. Maximum is 24 hours.

    -----

    Args:
        df (DF):
            An arbitrary dataframe.
        ctx (Context):
            The context object.

    Returns:
        DF:
            The same dataframe, but with all file paths replaced
            with openable links to the files.
    """

    return OperationNode(
        operation_id="fbbf20fa595c41b9bd7b11cb62a1ed1b",
        config=config,
        processor_id="get_links_to_files",
        package_id="utility",
        alias=alias,
    )

Registry().register("7da851efec4d46c18ed49d2175d5ab96", {'operation_id': '7da851efec4d46c18ed49d2175d5ab96', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'pattern_match_processor', 'processor_id': 'pattern_match_processor', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=MatchPattern)
def pattern_match_processor(
    dataframe: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    pattern: Annotated["str", ConfigArgument(required=True)] = None,
    join_char: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["MatchPattern"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """
    This processor finds all the fragments
    that match a certain pattern within each cell.

    They are later joined with a certain symbol and saved in an analogue cell

    ## Input:

        An arbitrary DataFrame

    ## Configuration:
        - `pattern`: str.
        A regular expression pattern to match.
        - `join_char`: str, default ';'.
        A character to join the matches with.

    ## Output:

        Result DataFrame

    -----

    Args:
        dataframe: the input
        context: the usual Malevich context, we expect the field 'pattern' from it.

    Returns: a new dataframe with the same dimensions and names as the input dataframe.
    """

    return OperationNode(
        operation_id="7da851efec4d46c18ed49d2175d5ab96",
        config=config,
        processor_id="pattern_match_processor",
        package_id="utility",
        alias=alias,
    )

Registry().register("193c5b5dac79412fb9e0edaada967902", {'operation_id': '193c5b5dac79412fb9e0edaada967902', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'merge', 'processor_id': 'merge', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=True, config_model=Merge)
def merge(
    *dfs: Any, 
    how: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    both_on: Annotated["typing.Union[str, typing.List, NoneType]", ConfigArgument(required=False)] = None,
    left_on: Annotated["typing.Union[str, typing.List, NoneType]", ConfigArgument(required=False)] = None,
    right_on: Annotated["typing.Union[str, typing.List, NoneType]", ConfigArgument(required=False)] = None,
    suffixes: Annotated["typing.Optional[typing.List]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["Merge"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Merges multiple dataframes into one

    ## Input:
        An iterable containing multiple dataframes to be merged.

    ## Configuration:
        - `how`: str, default 'inner'.
            The type of merge to be performed.

            Possible values:
                'inner':
                    Use intersection of keys from both frames,
                    similar to a SQL inner join;
                'outer':
                    Use union of keys from both frames,
                    similar to a SQL full outer join;
                'left':
                    Use only keys from left frame,
                    similar to a SQL left outer join;
                'right':
                    Use only keys from right frame,
                    similar to a SQL right outer join;
                'cross':
                    Create a cartesian product from both frames,
                    similar to a SQL cross join.

        - `both_on`: str|tuple, default ''.
            Column name or 'index' to merge on. If 'index', the index of the dataframe will be used. If column name, the column should be present in all dataframes.
        - `left_on`: str|list, default ''.
            Column name or 'index' to join on in the left DataFrame. If 'index', the index of the dataframe will be used. If column name, the column should be present in all but last dataframes.
        - `right_on`: str|list, default ''.
            Column name or 'index' to join on in the right DataFrame. If 'index', the index of the dataframe will be used. If column name, the column should be present in all but first dataframes.
        - `suffixes`: tuple, default ('_0', '_1').
            Suffix to apply to overlapping column names in the left and right dataframes.

    ## Output:
        Merged DataFrame

    ## Notes:
        If both 'both_on' and 'left_on/right_on' are provided,
        'both_on' will be ignored.

        Dataframes are merged iteratively from left to right.

        If using left_on column, all dataframes except
        the last one should have the column.

        If using right_on column, all dataframes except
        the first one should have the column.

    -----

    Args:
        dfs: DFS containing DataFrames to be merged.

    Returns:
        The merged dataframe
    """

    return OperationNode(
        operation_id="193c5b5dac79412fb9e0edaada967902",
        config=config,
        processor_id="merge",
        package_id="utility",
        alias=alias,
    )

Registry().register("153b9e1dab7248189aa7dc326ed83001", {'operation_id': '153b9e1dab7248189aa7dc326ed83001', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'pass_through', 'processor_id': 'pass_through', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=None)
def pass_through(
    df: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    alias: Optional["str"] = None,
    config: Optional[dict] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """
    Passes df to the next app

    ## Input:

        Arbitrary DataFrame

    ## Output:

        Arbitrary DataFrame

    -----
    Args:
        df (DF): Arbitrary DataFrame
    Returns:
        df (DF): Arbitrary DataFrame
    """

    return OperationNode(
        operation_id="153b9e1dab7248189aa7dc326ed83001",
        config=config,
        processor_id="pass_through",
        package_id="utility",
        alias=alias,
    )

Registry().register("967abe6f3676423f9660ce9fff49805b", {'operation_id': '967abe6f3676423f9660ce9fff49805b', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'rename', 'processor_id': 'rename', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=None)
def rename(
    df: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    alias: Optional["str"] = None,
    config: Optional[dict] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Renames columns in a dataframe.

    ## Input:
        DataFrame with the columns to be renamed.

    ## Output:
        DataFrame with renamed column

    ## Configuration:
        Provides mapping of old column names to their new names.

        For example, if the dataframe has columns 'a', 'b', 'c' and we want to rename
        'a' to 'A', 'b' to 'B', and 'c' to 'C', the configuration should be:

        {
            'a': 'A',
            'b': 'B',
            'c': 'C'
        }


        Provides mapping of old column names to their new names. For example 'a': 'A'.

    ## Details:
        This processor renames columns in the dataframe based on provided mappings.
        User needs to provide a dictionary in the configuration hat specifies old
        column names as keys and new column names as values.

    -----

    Args:
        df: DataFrame in which to rename columns.

    Returns:
        DataFrame with renamed columns.
    """

    return OperationNode(
        operation_id="967abe6f3676423f9660ce9fff49805b",
        config=config,
        processor_id="rename",
        package_id="utility",
        alias=alias,
    )

Registry().register("d1f1ca3ec35c47ec9e9ed594fb82341c", {'operation_id': 'd1f1ca3ec35c47ec9e9ed594fb82341c', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 's3_save', 'processor_id': 's3_save', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=S3Save)
def s3_save(
    dfs: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    names: Annotated["typing.Union[typing.List[str], str]", ConfigArgument(required=True)] = None,
    append_run_id: Annotated["typing.Optional[bool]", ConfigArgument(required=False)] = None,
    extra_str: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    aws_access_key_id: Annotated["str", ConfigArgument(required=True)] = None,
    aws_secret_access_key: Annotated["str", ConfigArgument(required=True)] = None,
    bucket_name: Annotated["str", ConfigArgument(required=True)] = None,
    endpoint_url: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    aws_region: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["S3Save"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Saves dataframes to S3.

    ## Input:
        dfs: Multiple dataframes to be saved.

    ## Configuration:
        - `names`: list[str]|str.
            Names of the dataframes to be saved.
            If a list is provided, it should have the same length as the number of dataframes.
            If a string is provided, it is used as a format string to generate the names of the dataframes.
            Available format variables:
                {ID}: index of the dataframe
            If the number of dataframes is greater than the length of the list or
            the number of format variables in the string, default names are used
            for the remaining dataframes.
        - `append_run_id`: bool, default False.
            If True, the run_id is appended to the names of the dataframes.
        - `extra_str`: str, default None.
            If provided, it is appended to the names of the dataframes.

        Also, the app should be provided with parameters to connect to S3:
        - `aws_access_key_id`: str.
            AWS access key ID.
        - `aws_secret_access_key`: str.
            AWS secret access key.
        - `bucket_name`: str.
            Name of the S3 bucket.
        - `endpoint_url`: str, default None.
            Endpoint URL of the S3 bucket.
        - `aws_region`: str, default None.
            AWS region of the S3 bucket.

    ## Details:
        This processor saves dataframes to S3. User can provide names for the
        dataframes to be saved. If no names are provided, default names are used.
        If the number of dataframes is greater than the length of the list of names
        or the number of format variables in the string, default names are used
        for the remaining dataframes.

        If `append_run_id` is True, the run_id is appended to the names of the
        dataframes. If `extra_str` is provided, it is appended to the names of
        the dataframes.

        The dataframes are saved to S3 using the following key:
            <EXTRA_STR>/<RUN_ID>/<NAME>

        A common use case of extra_str is to save dataframes to different folders
        within the S3 bucket. For example, if extra_str is 'train', the dataframes
        are saved to the following key:

            train/<RUN_ID>/<NAME>

    ## Output:
        The same as the input.

    -----

    Args:
        dfs: DFS containing DataFrames to be saved.

    Returns:
        The same dataframes as the input.

    """

    return OperationNode(
        operation_id="d1f1ca3ec35c47ec9e9ed594fb82341c",
        config=config,
        processor_id="s3_save",
        package_id="utility",
        alias=alias,
    )

Registry().register("a2595ce16ca9470599ce58931c6eb1c8", {'operation_id': 'a2595ce16ca9470599ce58931c6eb1c8', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 's3_save_files_auto', 'processor_id': 's3_save_files_auto', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=S3SaveFilesAuto)
def s3_save_files_auto(
    files: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    append_run_id: Annotated["typing.Optional[bool]", ConfigArgument(required=False)] = None,
    extra_str: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    aws_access_key_id: Annotated["str", ConfigArgument(required=True)] = None,
    aws_secret_access_key: Annotated["str", ConfigArgument(required=True)] = None,
    bucket_name: Annotated["str", ConfigArgument(required=True)] = None,
    endpoint_url: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    aws_region: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["S3SaveFilesAuto"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Saves files from local file system to S3 preserving the original names.

    ## Input:

        A dataframe with one column:
            - `filename` (str): Contains the names of the files to be saved.

    ## Configuration:
        - `append_run_id`: bool, default False.
            If True, the run_id is appended to the names of the files.
        - `extra_str`: str, default None.
            If provided, it is appended to the names of the files.

        Also, the app should be provided with parameters to connect to S3:
        - `aws_access_key_id`: str.
            AWS access key ID.
        - `aws_secret_access_key`: str.
            AWS secret access key.
        - `bucket_name`: str.
            Name of the S3 bucket.
        - `endpoint_url`: str, default None.
            Endpoint URL of the S3 bucket.
        - `aws_region`: str, default None.
            AWS region of the S3 bucket.

    ## Details:
        Files are expected to be in the share folder e.g. should be shared with
        `context.share(<FILE>)` before by a previous processor.

        The files are saved to S3 using the following key:
            <EXTRA_STR>/<RUN_ID>/<SHARED_FILE_NAME>

        A common use case of extra_str is to save files to different folders
        within the S3 bucket. For example, if extra_str is 'train', the files
        are saved to the following key:

                train/<RUN_ID>/<SHARED_FILE_NAME>

    ## Output:
        The same as the input.

    -----

    Args:
        files: DF containing filenames to be saved.

    Returns:
        The same dataframe as the input.
    """

    return OperationNode(
        operation_id="a2595ce16ca9470599ce58931c6eb1c8",
        config=config,
        processor_id="s3_save_files_auto",
        package_id="utility",
        alias=alias,
    )

Registry().register("78f1d737e5d142b7b425e14b225c9b13", {'operation_id': '78f1d737e5d142b7b425e14b225c9b13', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 's3_save_files', 'processor_id': 's3_save_files', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=S3SaveFiles)
def s3_save_files(
    files: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    append_run_id: Annotated["typing.Optional[bool]", ConfigArgument(required=False)] = None,
    aws_access_key_id: Annotated["str", ConfigArgument(required=True)] = None,
    aws_secret_access_key: Annotated["str", ConfigArgument(required=True)] = None,
    bucket_name: Annotated["str", ConfigArgument(required=True)] = None,
    endpoint_url: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    aws_region: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["S3SaveFiles"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Saves files from local file system to S3.

    ## Input:
        A dataframe with columns:
        - `filename` (str): the names of the files to be saved.
        - `s3key` (str): the S3 key for each file.

    ## Configuration:
        - `append_run_id`: bool, default False.
            If True, the run_id is appended to the names of the files.

        Also, the app should be provided with parameters to connect to S3:
        - `aws_access_key_id`: str.
            AWS access key ID.
        - `aws_secret_access_key`: str.
            AWS secret access key.
        - `bucket_name`: str.
            Name of the S3 bucket.
        - `endpoint_url`: str, default None.
            Endpoint URL of the S3 bucket.
        - `aws_region`: str, default None.
            AWS region of the S3 bucket.

    ## Details:
        Files are expected to be in the share folder e.g. should be shared with
        `context.share(<FILE>)` before by a previous processor.

        The files are saved to S3 using the following key:
            <EXTRA_STR>/<RUN_ID>/<S3_KEY>

        A common use case of extra_str is to save files to different folders
        within the S3 bucket. For example, if extra_str is 'train', the files
        are saved to the following key:

                train/<RUN_ID>/<S3_KEY>

        S3 keys might contain following variables:
            - {ID}: index of the dataframe
            - {FILE}: base filename, for example 'file.csv' for 'path/to/file.csv'
            - {RUN_ID}: run_id

        For example, if the S3 key is 'train/{RUN_ID}/{FILE}', file name is 'file.csv',
        run_id is 'run_1', the file will be saved to 'train/run_1/file.csv'.

    ## Output:
        The same as the input.

    -----

    Args:
        files: DF containing filenames to be saved.

    Returns:
        The same dataframe as the input.
    """

    return OperationNode(
        operation_id="78f1d737e5d142b7b425e14b225c9b13",
        config=config,
        processor_id="s3_save_files",
        package_id="utility",
        alias=alias,
    )

Registry().register("62e0f497c63048d8b7138199e44f6cdd", {'operation_id': '62e0f497c63048d8b7138199e44f6cdd', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 's3_download_files', 'processor_id': 's3_download_files', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=S3DownloadFiles)
def s3_download_files(
    files: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    aws_access_key_id: Annotated["str", ConfigArgument(required=True)] = None,
    aws_secret_access_key: Annotated["str", ConfigArgument(required=True)] = None,
    bucket_name: Annotated["str", ConfigArgument(required=True)] = None,
    endpoint_url: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    aws_region: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["S3DownloadFiles"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Downloads files from S3 to local file system.

    ## Input:
        A dataframe with columns:
        - `filename` (str): the names of the files to be saved.
        - `s3key` (str): the S3 key for each file.

    ## Configuration:

        The app's only configuration is the connection to S3:
        - `aws_access_key_id`: str.
            AWS access key ID.
        - `aws_secret_access_key`: str.
            AWS secret access key.
        - `bucket_name`: str.
            Name of the S3 bucket.
        - `endpoint_url`: str, default None.
            Endpoint URL of the S3 bucket.
        - `aws_region`: str, default None.
            AWS region of the S3 bucket.

    ## Details:
       Files are downloaded by their S3 keys. The files are shared across processors
       under keys specified by `filename` column.

       For example, for the dataframe:
            | filename  |            s3key          |
            | --------  |            -----          |
            | file1.csv | path/to/some_file.csv     |

            The file is assumed to be in S3 under the key `path/to/some_file.csv`.
            The file is downloaded from S3 and shared under the key `file1.csv`.

    ## Output:
        The dataframe with downloaded filenames.

    -----

    Args:
        files: DF containing filenames to be downloaded.

    Returns:
        The dataframe with downloaded filenames.
    """

    return OperationNode(
        operation_id="62e0f497c63048d8b7138199e44f6cdd",
        config=config,
        processor_id="s3_download_files",
        package_id="utility",
        alias=alias,
    )

Registry().register("5bb16b362f15427097d25f2f9f3d3fd7", {'operation_id': '5bb16b362f15427097d25f2f9f3d3fd7', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 's3_download_files_auto', 'processor_id': 's3_download_files_auto', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=S3DownloadFilesAuto)
def s3_download_files_auto(
    keys: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    aws_access_key_id: Annotated["str", ConfigArgument(required=True)] = None,
    aws_secret_access_key: Annotated["str", ConfigArgument(required=True)] = None,
    bucket_name: Annotated["str", ConfigArgument(required=True)] = None,
    endpoint_url: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    aws_region: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["S3DownloadFilesAuto"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Downloads files from S3 to local file system.

    ## Input:
        A dataframe with columns:
        - `s3key` (str): the S3 key for each file.

    ## Configuration:

        The app's only configuration is the connection to S3:

        - `aws_access_key_id`: str.
            AWS access key ID.
        - `aws_secret_access_key`: str.
            AWS secret access key.
        - `bucket_name`: str.
            Name of the S3 bucket.
        - `endpoint_url`: str, default None.
            Endpoint URL of the S3 bucket.
        - `aws_region`: str, default None.
            AWS region of the S3 bucket.

    ## Output:
        A dataframe with columns:
            - s3key (str): S3 key of the file
            - filename (str): The name of the file

    -----

    Args:
        keys: DF containing keys of the files to be downloaded.

    Returns:
        A dataframe with columns:
            - s3key (str): S3 key of the file
            - filename (str): The name of the file
    """

    return OperationNode(
        operation_id="5bb16b362f15427097d25f2f9f3d3fd7",
        config=config,
        processor_id="s3_download_files_auto",
        package_id="utility",
        alias=alias,
    )

Registry().register("2432b82f14b94a48b86bd672334270a0", {'operation_id': '2432b82f14b94a48b86bd672334270a0', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'filter', 'processor_id': 'filter', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=Filter)
def filter(
    df: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    conditions: Annotated["typing.Optional[typing.List[typing.Dict[str, typing.Any]]]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["Filter"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Filters rows by a number of conditions

    ## Input:
        An arbitrary dataframe to filter rows from

    ## Output:
        A filtered dataframe

    ## Configuration:

        - `conditions`: list[dict], default [].
            A list of conditions containing dictionaries.
        A list of conditions containing dictionaries with the following keys:

        `column`: str.
            The column to filter on.
        `operation`: str.
            The operation to perform.
        `value`: any.
            The value to filter on.
        `type`: str.
            The type of the value to filter on (optional).

    ## Example:
    {
        "conditions": [
            {
                "column": "age",
                "operation": "greater",
                "value": 18,
                "type": "int"
            },
            {
                "column": "name",
                "operation": "like",
                "value": "John"
            }
        ]
    }

    Supported operations:
    - equal
    - not_equal
    - greater
    - greater_equal
    - less
    - less_equal
    - in
    - not_in
    - like
    - not_like
    - is_null
    - is_not_null

    Supported types:
    - int
    - float
    - bool
    - str

    -----

    Args:
        df: the dataframe to filter rows from
        context: the context of the current request

    Returns:
        A filtered dataframe
    """

    return OperationNode(
        operation_id="2432b82f14b94a48b86bd672334270a0",
        config=config,
        processor_id="filter",
        package_id="utility",
        alias=alias,
    )

Registry().register("f5e3add155aa403a9b5f8f698f6fe84a", {'operation_id': 'f5e3add155aa403a9b5f8f698f6fe84a', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'locs', 'processor_id': 'locs', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=Locs)
def locs(
    df: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    column: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    columns: Annotated["typing.Optional[typing.List[str]]", ConfigArgument(required=False)] = None,
    column_idx: Annotated["typing.Optional[int]", ConfigArgument(required=False)] = None,
    column_idxs: Annotated["typing.Optional[typing.List[int]]", ConfigArgument(required=False)] = None,
    row: Annotated["typing.Optional[int]", ConfigArgument(required=False)] = None,
    rows: Annotated["typing.Optional[typing.List[int]]", ConfigArgument(required=False)] = None,
    row_idx: Annotated["typing.Optional[int]", ConfigArgument(required=False)] = None,
    row_idxs: Annotated["typing.Optional[typing.List[int]]", ConfigArgument(required=False)] = None,
    unique: Annotated["typing.Optional[bool]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["Locs"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """ Locate Statically - Extracts a subset of the dataframe

    ## Input:
        A DataFrame to be processed.

    ## Output:
        A DataFrame with requested columns

    ## Configuration:
        The app configuration should contain at least one of the following fields:

        - `column`: str, default None.
          The column to be extracted.
        - `columns`: list[str], default None.
          The columns to be extracted.
        - `column_idx`: int, default None.
            The column index to be extracted.
        - `column_idxs`: list[int], default None.
            The column indexes to be extracted.
        - `row`: int, default None.
            The row to be extracted.
        - `rows`: list[int], default None.
            The rows to be extracted.
        - `row_idx`: int, default None.
            The row index to be extracted.
        - `row_idxs`: list[int], default None.
            The row indexes to be extracted.
        - `unique`: bool, default False.
            Get unique values from column. Must be used with `column` or `column_idx`.

        Multiple fields may be provided and in such case,
        the function will extract the intersection of the fields.

    ## Notes:
        At least one of the above fields should be provided for the function to work.

        Moreover, the dataframe is processed in column first then row order.
        Queries are executed from the most specific to the least within each category.
        If both specific and general conditions are given, the function prioritizes
        the specific ones to maintain consistency.

    -----

    Args:
        df (pd.DataFrame): The DataFrame to be processed.

    Returns:
        The extracted subset from the DataFrame.
    """

    return OperationNode(
        operation_id="f5e3add155aa403a9b5f8f698f6fe84a",
        config=config,
        processor_id="locs",
        package_id="utility",
        alias=alias,
    )

Registry().register("c1e4ceb1c75e4309918d4e7f45d5ef19", {'operation_id': 'c1e4ceb1c75e4309918d4e7f45d5ef19', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'subset', 'processor_id': 'subset', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=Subset)
def subset(
    dfs: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    expr: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["Subset"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Select a subset of dataframes from the list of dataframes.

    ## Input:
        A number of dataframes.

    ## Output:
        A subset of dataframes or a single dataframe.

    ## Configuration:
        - `expr`: str, default None.
            A comma-separated list of integers or slices, e.g. `0,1:3,5:7,6,9:10`. The first dataframe has index 0.

    ## Details:
        The `expr` field should be a comma-separated list of integers or slices,
        e.g. `0,1:3,5:7,6,9:10`.

        Zero-based indexing is used for the dataframes.

        `expr` is matched against the regular expression
            `^(\d+|(\d+\:\d+))(\,(\d+|(\d+\:\d+)))*$`.

        If the expression contains only one element, a single dataframe is
        returned. Otherwise, a slice of dataframes is returned.

    -----

    Args:
        dfs: A number of arbitrary dataframes.

    Returns:
        A subset of dataframes or a single dataframe if the subset contains
        a single index.
    """

    return OperationNode(
        operation_id="c1e4ceb1c75e4309918d4e7f45d5ef19",
        config=config,
        processor_id="subset",
        package_id="utility",
        alias=alias,
    )

Registry().register("0895da002f764f50a23569b04a9bf8b5", {'operation_id': '0895da002f764f50a23569b04a9bf8b5', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'squash_rows', 'processor_id': 'squash_rows', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=SquashRows)
def squash_rows(
    df: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    by: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    delim: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["SquashRows"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Squash multiple rows into one row.

    ## Input:
        An arbitrary dataframe with columns that contain multiple values.

    ## Output:
        A dataframe with the same columns as the input dataframe, but with
        multiple rows for each input row.

    ## Configuration:
        - `by`: str, default 'all'.
            The column to group by. If not specified, all columns will be squashed.

        - `delim`: str, default ','.
            The delimiter used to separate values in the columns. If not specified, the default delimiter is a comma (,).

    -----

    Args:
        df (DF[Any]): Dataframe
        context (Context): Context object

    Returns:
        Dataframe with squashed rows
    """

    return OperationNode(
        operation_id="0895da002f764f50a23569b04a9bf8b5",
        config=config,
        processor_id="squash_rows",
        package_id="utility",
        alias=alias,
    )

Registry().register("3ef41e86189744e0b3dbb13a5b5107da", {'operation_id': '3ef41e86189744e0b3dbb13a5b5107da', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'squash', 'processor_id': 'squash', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=Squash)
def squash(
    df: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    by: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    delim: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["Squash"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Squash multiple rows into one row.

    ## Input:
        An arbitrary dataframe with columns that contain multiple values.

    ## Output:
        A dataframe with the same columns as the input dataframe, but with
        multiple rows for each input row.

    ## Configuration:
        - `by`: str, default 'all'.
            The column to group by. If not specified, all columns will be squashed.

        - `delim`: str, default ','.
            The delimiter used to separate values in the columns. If not specified, the default delimiter is a comma (,).

    -----

    Args:
        df (DF[Any]): Dataframe
        context (Context): Context object

    Returns:
        Dataframe with squashed rows
    """

    return OperationNode(
        operation_id="3ef41e86189744e0b3dbb13a5b5107da",
        config=config,
        processor_id="squash",
        package_id="utility",
        alias=alias,
    )

Registry().register("ebd00a45aae84a50be5f9eae3f75df1c", {'operation_id': 'ebd00a45aae84a50be5f9eae3f75df1c', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'squash_columns', 'processor_id': 'squash_columns', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=SquashColumns)
def squash_columns(
    df: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    columns: Annotated["typing.Optional[typing.List[str]]", ConfigArgument(required=False)] = None,
    result_column_name: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    drop: Annotated["typing.Optional[bool]", ConfigArgument(required=False)] = None,
    delim: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["SquashColumns"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Squash multiple columns into one column.

    ## Input:
        An arbitrary dataframe.

    ## Output:
        A dataframe with the same rows as the input dataframe, but with
        specified columns squashed into one column.

    ## Configuration:
        - `columns`: list[str], default None.
            The columns to squash. If not specified, all columns will be squashed.
        - `result_column_name`: str, default None.
            The name of the resulting column. If not specified, the default name is the concatenation of the column names.
        - `drop`: bool, default False.
            Whether to drop the original columns. If not specified, the default value is False.
        - `delim`: str, default ','.
            The delimiter used to separate values in the columns. If not specified, the default delimiter is a comma (,).

    -----

    Args:
        df (DF[Any]): An input collection
        config (Context): Configuration object

    Returns:
        Dataframe with squashed columns
    """

    return OperationNode(
        operation_id="ebd00a45aae84a50be5f9eae3f75df1c",
        config=config,
        processor_id="squash_columns",
        package_id="utility",
        alias=alias,
    )

Registry().register("47dc1bf2998f478fbd4afcce767e14e7", {'operation_id': '47dc1bf2998f478fbd4afcce767e14e7', 'reverse_id': 'utility', 'branch': '5dacb7e7ae004ea188fa5a256cd36606', 'version': '9f2f1af1b4484dcabaa225fef73ce45d', 'processor_name': 'unwrap', 'processor_id': 'unwrap', 'image_ref': ('dependencies', 'utility', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'utility', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'utility', 'options', 'image_auth_pass')})

@proc(use_sinktrace=False, config_model=Unwrap)
def unwrap(
    df: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    columns: Annotated["typing.Optional[typing.List[str]]", ConfigArgument(required=False)] = None,
    delimiter: Annotated["typing.Optional[str]", ConfigArgument(required=False)] = None,
    alias: Optional["str"] = None,
    config: Optional["Unwrap"] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """Unwrap columns with multiple values into multiple rows.

    If a column contains multiple values, this processor will create a new row
    for each value. The new rows will be identical to the original row except
    for the column that was unwrapped.

    For example, if the input dataframe is:

    | id | name | tags |
    |----|------|------|
    | 1  | A    | a,b  |
    | 2  | B    | c    |

    Then the output dataframe will be:

    | id | name | tags |
    |----|------|------|
    | 1  | A    | a    |
    | 1  | A    | b    |
    | 2  | B    | c    |


    ## Input:

        An arbitrary dataframe with columns that contain multiple values.

    ## Output:

        A dataframe with the same columns as the input dataframe, but with
        multiple rows for each input row.

    ## Configuration:

        - `columns`: list[str], default ['all'].
            The columns to unwrap. If not specified, all columns will be unwrapped.

        - `delimiter`: str, default ','.
            The delimiter used to separate values in the columns. If not specified, the default delimiter is a comma (,).

    ## Notes:

        Be careful when using this processor with columns that contain
        non-text values. For example, if a column contains a list of numbers,
        and the delimiter is a dot, then the processor will treat each number
        as a separate value. For example, if the input dataframe is:

    | id | name | numbers |
    |----|------|---------|
    | 1  | A.B  | 1.2     |

        Then the output dataframe will be:

    | id | name | numbers |
    |----|------|---------|
    | 1  | A    | 1       |
    | 1  | A    | 2       |
    | 1  | B    | 1       |
    | 1  | B    | 2       |

    -----

    Args:

        df (pandas.DataFrame): The input dataframe.

        config (dict): The configuration for this processor.

    Returns:

        The same dataframe as the input dataframe, but with multiple rows for
        each input row.
    """

    return OperationNode(
        operation_id="47dc1bf2998f478fbd4afcce767e14e7",
        config=config,
        processor_id="unwrap",
        package_id="utility",
        alias=alias,
    )
