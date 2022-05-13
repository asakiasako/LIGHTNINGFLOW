from dataclasses import dataclass
from io import TextIOWrapper
import sys
from typing import Optional

@dataclass(frozen=True)
class Environment:
    outputTarget: TextIOWrapper = sys.stdout
    currentTask: Optional[str] = None
    currentJob: Optional[str] = None
    currentWorkflow: Optional[str] = None

