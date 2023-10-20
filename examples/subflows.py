from pandas import DataFrame

from malevich import collection, config, flow
from malevich.constants import DEFAULT_CORE_HOST
from malevich.interpreter.core import CoreInterpreter
from malevich.utility import add_column, merge_two


@flow()
def subflow(x):
    return add_column(
        x,
        config(column="first column", value="value in the first column")
    )


@flow()
def yet_another_subflow(x):
    return add_column(
        x,
        config(column="another column", value="value in the another column")
    )


@flow()
def main_flow():
    data = collection(data=DataFrame({'x': [1, 2]}))
    subflow_result = subflow(data)
    another_subflow_result = yet_another_subflow(data)
    return merge_two(
        subflow_result, 
        another_subflow_result,
        config(both_on='x')
    )


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser("Malevich Subflow Example")
    parser.add_argument('--core-user', '-cu', required=True)
    parser.add_argument('--core-pass', '-cp', required=True)
    parser.add_argument('--core-host', '-ch', default=DEFAULT_CORE_HOST, required=False)

    args = parser.parse_args()

    # That is a 'promised' flow
    # which means it is already
    # parsed, but not yet interpreted
    promise = main_flow()

    # Now, let us interpret our
    # meta flow using Malevich Core API
    promise.interpret(
        CoreInterpreter(
            core_auth=(args.core_user, args.core_pass),
            core_host=args.core_host,
        )
    )

    # Run task and print results
    print(promise())
