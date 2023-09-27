def test_simple_tree_1(): # noqa: ANN201
    import malevich._autoflow.tracer as gn  # engine

    def boo(x: gn.tracer, y: gn.multitracer) -> gn.tracer:
        # Create an instance that
        # 'owns' boo function
        app = gn.tracer('boo')
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
        app = gn.tracer('foo')

        # Put an information that
        # `boo` was called after
        # current owner of x is called
        input._autoflow.calledby(app)

        # Return owned entity to track
        return app



    with gn.Flow() as tree:
        # Traced input
        x = gn.tracer('@')
        y = gn.multitracer('@')

        a = boo(x, y)
        foo(a)

        tree_ = [*tree.traverse()]
        assert ('@', 'boo') in tree_
        assert ('boo', 'foo') in tree_
        assert len(tree_) == 3
