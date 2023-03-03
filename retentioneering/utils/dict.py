def clear_dict(d: dict) -> dict:
    for k, v in dict(d).items():
        if v is None:
            del d[k]
    return d
