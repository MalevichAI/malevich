import pandas as pd
from jls_utils import task_run, task_stop

from malevich import collection, pipeline
from malevich.utility import add_column


@pipeline(interpreter='core')
def my_pipeline():  # noqa: ANN201
    data = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )

    mycollection = collection(name='mycollection', data=data)
    add_column(mycollection, config={"column_name": "c", "value": 7})


if __name__ == "__main__":
    task_id = my_pipeline()
    task_run(task_id, with_logs=True)
    task_stop(task_id)
