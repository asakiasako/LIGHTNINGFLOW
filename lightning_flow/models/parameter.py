from abc import ABC, abstractmethod
from typing import Any


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