from uuid import uuid4

import malevich._autoflow.tracer as gn  # engine


def boo(x: gn.tracer, y: gn.multitracer) -> gn.tracer:
    # Create an instance that
    # 'owns' boo function
    app = gn.tracer(uuid4().hex)
    # Put an information that
    # `boo` was called after
    # current owner of x is called
    x._autoflow.calledby(app)

    # The same for y, but
    # y is collection of entities
    y._autoflow.calledby(app)

    # Return owned entity to track
    return app


def foo(input: gn.tracer) -> gn.tracer:
    # Create an instance that
    # 'owns' boo function
    app = gn.tracer(uuid4().hex)

    # Put an information that
    # `boo` was called after
    # current owner of x is called
    input._autoflow.calledby(app)

    # Return owned entity to track
    return app



with gn.Flow() as tree:
    # Traced input
    x = gn.tracer() # @ is marker for root element
    y = gn.multitracer()

    a = boo(x, y)
    b = foo(a)

    for node in tree.traverse():
        print(node)
