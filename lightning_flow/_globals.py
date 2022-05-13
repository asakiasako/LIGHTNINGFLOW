from dataclasses import dataclass
from io import TextIOWrapper
import sys
from typing import Optional

__all__ = ['globalParams']

@dataclass(frozen=True)
class GlobalParams:
    outputTarget: TextIOWrapper = sys.stdout
    currentNode: Optional[str] = None

globalParams = GlobalParams()
