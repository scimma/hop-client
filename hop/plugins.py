import pluggy


# hooks for registering plugins
register = pluggy.HookimplMarker("hop")
specification = pluggy.HookspecMarker("hop")


@specification
def get_models():
    """
    This plugin spec is used to return message models in the form:
        {"type": Model}

    where the type refers to a specific message model.

    """
