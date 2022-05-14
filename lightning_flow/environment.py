from dataclasses import dataclass
from io import TextIOWrapper
import sys
from typing import Optional

@dataclass
class Environment:
    """A singoten class to store environments globally."""

    __instance = None

    def __new__(cls: type):
        cls.__instance = cls.__instance or object.__new__(cls)
        return cls.__instance

    outputTarget: TextIOWrapper = sys.stdout
    currentTask: Optional[str] = None
    currentJob: Optional[str] = None
    currentWorkflow: Optional[str] = None
