import sys, traceback

from .environment import Environment


env = Environment()


def get_tb_str():
    err_type, err, tb = sys.exc_info()
    err_info = []
    err_info.append('Traceback (most recent call last):\n')
    err_info.extend(traceback.format_tb(tb))
    err_str = f"{err_type.__name__}\n"
    if str(err):
        err_str = f"{err_type.__name__}: {str(err)}\n"
    err_info.append(err_str)
    return ''.join(err_info)


def output(msg: str, *, level='info'):
    if level not in {'info', 'warning', 'error'}:
        raise ValueError(f"{level!r} is not a valid level")
    t = f"[{level}] {msg}\n"
    env.outputTarget.write(t)
    if hasattr(env.outputTarget, 'flush'):
        env.outputTarget.flush()
    