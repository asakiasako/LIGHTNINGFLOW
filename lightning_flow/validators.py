from __future__ import annotations
from typing import Iterable, Type
import os


def _listify_validator(scalar_validator, *, n=None, doc=None):

    def f(s):
        if isinstance(s, str):
            val = [scalar_validator(v.strip()) for v in s.split(',')
                    if v.strip()]
        # Allow any ordered sequence type -- generators, np.ndarray, pd.Series
        # -- but not sets, whose iteration order is non-deterministic.
        elif isinstance(s, Iterable) and not isinstance(s, (set, frozenset)):
            val = [scalar_validator(v) for v in s]
        else:
            raise ValueError(
                f"Expected str or other non-set iterable, but got {s}")
        if n is not None and len(val) != n:
            raise ValueError(
                f"Expected {n} values, but there are {len(val)} values in {s}")
        return val

    try:
        f.__name__ = "{}list".format(scalar_validator.__name__)
    except AttributeError:  # class instance.
        f.__name__ = "{}List".format(type(scalar_validator).__name__)
    f.__qualname__ = f.__qualname__.rsplit(".", 1)[0] + "." + f.__name__
    f.__doc__ = doc if doc is not None else scalar_validator.__doc__
    return f


def validate_any(s):
    return s


validate_anylist = _listify_validator(validate_any)


def _make_type_validator(cls: Type, *, allow_none=False) -> function:
    """
    Return a validator that converts inputs to *cls* or raises (and possibly
    allows ``None`` as well).
    """
    def validator(s):
        if allow_none and s is None:
            return None
        if cls is str and not isinstance(s, str):
            raise TypeError(f"{s} is not type str")
        if cls is int:
            try:
                val = round(s)
                if val != s:
                    raise ValueError(f"Could not convert {s!r} to int without precision loss")
            except TypeError:
                pass
        try:
            return cls(s)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f'Could not convert {s!r} to {cls.__name__}') from e

    validator.__name__ = f"validate_{cls.__name__}"
    if allow_none:
        validator.__name__ += "_or_None"
    validator.__qualname__ = (
        validator.__qualname__.rsplit(".", 1)[0] + "." + validator.__name__)
    return validator


validate_str = _make_type_validator(str)
validate_str_or_None = _make_type_validator(str, allow_none=True)
validate_strlist = _listify_validator(
    validate_str, doc='return a list of strings')
validate_int = _make_type_validator(int)
validate_int_or_None = _make_type_validator(int, allow_none=True)
validate_intlist = _listify_validator(
    validate_int, doc='return a list of ints')
validate_float = _make_type_validator(float)
validate_float_or_None = _make_type_validator(float, allow_none=True)
validate_floatlist = _listify_validator(
    validate_float, doc='return a list of floats')


def validate_bool(s):
    if s not in (True, False):
        raise ValueError(f"Could not convert {s!r} to bool")


def _make_range_validator(*, _min: int | float = float('-inf'), _max: int | float = float('inf')):

    def validate_range(s):
        if not _min <= s <= _max:
            raise ValueError(f"{s!r} is not in range from {_min!r} to {_max!r}")
        return s

    return validate_range


def _make_options_validator(*, options: Iterable = []):

    def validate_options(s):
        if s not in options:
            raise ValueError(f"{s!r} is not a valid option")
        return s

    return validate_options


def _validate_pathlike(s):
    if isinstance(s, (str, os.PathLike)):
        # Store value as str because savefig.directory needs to distinguish
        # between "" (cwd) and "." (cwd, but gets updated by user selections).
        return os.fsdecode(s)
    else:
        raise TypeError(f"Cound not convert {s!r} to path str")

