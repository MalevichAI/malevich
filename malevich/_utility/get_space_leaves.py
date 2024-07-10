from malevich_space.schema import LoadedComponentSchema, LoadedFlowSchema


def get_space_leaves(element: LoadedComponentSchema | LoadedFlowSchema):
    if isinstance(element, LoadedComponentSchema):
        flow = element.flow
    else:
        flow = element

    if flow is None:
        raise ValueError("Flow is not loaded")

    memory_ = set()
    for node in flow.components:
        if node.prev:
            for prev in node.prev:
                memory_.add(prev.uid)

    return [
        node for node in flow.components if node.uid not in memory_
    ]
