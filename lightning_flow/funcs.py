from .environment import globalParams

def output(msg: str, level='info'):
    if level not in {'info', 'warning', 'error'}:
        raise ValueError(f"{level!r} is not a valid level")
    target = globalParams.outputTarget
    header = f'[{level}] '
    t = f"{header}{msg}\n"
    target.write(t)
