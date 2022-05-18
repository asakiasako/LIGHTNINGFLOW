from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional

from . import validators


__all__ = [
    "Parameter",
    "AnyParameter",
    "IntParameter",
    "FloatParameter",
    "StrParameter",
    "BoolParameter",
    "ListParameter",
    "IntListParameter",
    "FloatListParameter",
    "StrListParameter",
    "OptionsParameter",
]


class Parameter(ABC):

    __count = 0

    def __init__(self):
        self.__class__.__count += 1
        self.__storage_name = f"_{self.__class__.__name__}#{self.__count}"
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def __set__(self, instance, value):
        value = self.validate(instance, value)
        setattr(instance, self.__storage_name, value)
        
    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            try:
                return getattr(instance, self.__storage_name)
            except AttributeError as e:
                raise AttributeError(f"This {self.__class__.__name__} of {instance.__class__.__name__} is not assigned yet.") from e

    @abstractmethod
    def validate(self, instance, value) -> Any:
        """Validate and returns the converted value."""


class AnyParameter(Parameter):

    def validate(self, instance, value) -> Any:
        return validators.validate_any(value)


class IntParameter(Parameter):
    
    def __init__(self, min: int | float = float('-inf'), max: int | float = float('inf'), allow_none: bool = False) -> None:
        super().__init__()
        self._min = min
        self._max = max
        self._allow_none = allow_none

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(min={self._min!r}, max={self._max}, allow_none={self._allow_none!r})"

    def validate(self, instance, value) -> Optional[int]:
        value = validators.validate_int_or_None(value) if self._allow_none else validators.validate_int(value)
        if value is not None:
            value = validators._make_range_validator(_min=self._min, _max=self._max)(value)
        return value


class FloatParameter(Parameter):

    def __init__(self, min: int | float = float('-inf'), max: int | float = float('inf'), allow_none: bool = False) -> None:
        super().__init__()
        self._min = min
        self._max = max
        self._allow_none = allow_none

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(min={self._min!r}, max={self._max!r}, allow_none={self._allow_none!r})"

    def validate(self, instance, value) -> Optional[float]:
        value = validators.validate_float_or_None(value) if self._allow_none else validators.validate_float(value)
        if value is not None:
            value = validators._make_range_validator(_min=self._min, _max=self._max)(value)
        return value


class StrParameter(Parameter):

    def __init__(self, allow_none: bool = False) -> None:
        super().__init__()
        self._allow_none = allow_none

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(allow_none={self._allow_none!r})"
    
    def validate(self, instance, value) -> Optional[str]:
        value = validators.validate_str_or_None(value) if self._allow_none else validators.validate_str(value)
        return value


class BoolParameter(Parameter):

    def validate(self, instance, value):
        return validators.validate_bool(value)


class ListParameter(Parameter):

    def __init__(self, n: Optional[int] = None):
        super().__init__()
        self._n = n

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(n={self._n!r})"
    
    def validate(self, instance, value):
        c_val = validators.validate_anylist(value)
        if self._n is not None:
            if len(c_val) != self._n:
                raise ValueError(f"Expected {self._n} values, but there are {len(c_val)} values in {value}")
        return c_val


class IntListParameter(Parameter):

    def __init__(self, n: Optional[int] = None):
        super().__init__()
        self._n = n

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(n={self._n!r})"

    def validate(self, instance, value):
        c_val = validators.validate_intlist(value)
        if self._n is not None:
            if len(c_val) != self._n:
                raise ValueError(f"Expected {self._n} values, but there are {len(c_val)} values in {value}")
        return c_val


class FloatListParameter(Parameter):

    def __init__(self, n: Optional[int] = None):
        super().__init__()
        self._n = n

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(n={self._n!r})"

    def validate(self, instance, value):
        c_val = validators.validate_floatlist(value)
        if self._n is not None:
            if len(c_val) != self._n:
                raise ValueError(f"Expected {self._n} values, but there are {len(c_val)} values in {value}")
        return c_val


class StrListParameter(Parameter):

    def __init__(self, n: Optional[int] = None):
        super().__init__()
        self._n = n

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(n={self._n!r})"

    def validate(self, instance, value):
        c_val = validators.validate_strlist(value)
        if self._n is not None:
            if len(c_val) != self._n:
                raise ValueError(f"Expected {self._n} values, but there are {len(c_val)} values in {value}")
        return c_val


class OptionsParameter(Parameter):

    def __init__(self, options=[]):
        super().__init__()
        self._options = list(options)

    def validate(self, instance, value):
        return validators._make_options_validator(options=self._options)(value)
