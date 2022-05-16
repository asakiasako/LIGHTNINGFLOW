from __future__ import annotations
from typing import Any, Iterable, Mapping
import sys, traceback
import json

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


def output(msg: str | Any, *, _type='info'):
    """Output to the output target of the current environment, and log the 
    message to the current running workflow.
    
    The default output target is sys.stdout, and can be changed by modifying 
    the lightning_flow.environment instance.

    The parameter _type is used to flag the msg type. If _type is 'json', it 
    will be converted to json string before output.
    
    Args:
        msg: The msg to output.
        _type: The type of the msg.
    """
    if _type not in {'info', 'warning', 'error', 'json'}:
        raise ValueError(f"{_type!r} is not a valid value for _type")
    if _type == 'json':
        msg = json.dumps(msg)
    t = f"[{_type}] {msg}\n"
    env.currentWorkflowInstance._log_output(output = t)
    env.outputTarget.write(t)
    if hasattr(env.outputTarget, 'flush'):
        env.outputTarget.flush()
