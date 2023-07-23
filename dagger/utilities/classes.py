def get_deep_obj_subclasses(obj) -> list:
    obj_subclasses: list = obj.__subclasses__()
    for subclass in obj_subclasses:
        obj_subclasses.extend(get_deep_obj_subclasses(subclass))
    return obj_subclasses
