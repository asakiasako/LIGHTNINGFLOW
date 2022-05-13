from collections import UserDict
from copy import deepcopy
from typing import Iterable, Mapping


class TaskData(UserDict):
    """ This class represents a dataset that is passed between tasks.
    It behaves like a dictionary but also contains a history of all tasks that have
    contributed to this dataset.
    """
    def __init__(self, data: Mapping = None, *, task_history: Iterable[str]=None) -> None:
        """
        Args:
            data: A Mapping with the initial data that should be stored.
            task_history: A list of task names that have contributed to this data.
        """
        self._data = dict(data) if data is not None else {}
        if isinstance(task_history, str):
            raise TypeError(f'{task_history} of type str is not valid for task_history. Need Iterable witch is not str.')
        self._task_history = list(task_history) if task_history is not None else []

    def add_task_history(self, task_name: str) -> None:
        """ Add a task name to the list of tasks that have contributed to this dataset.
        Args:
            task_name: The name of the task that contributed.
        """
        self._task_history.append(task_name)

    @property
    def data(self):
        return self._data

    @property
    def task_history(self):
        """ Return the list of task names that have contributed to this dataset.  """
        return self._task_history

    def __deepcopy__(self, memo):
        return TaskData(data=deepcopy(self._data, memo),
                        task_history=self._task_history[:])

    def __repr__(self):
        return f'{self.__class__.__name__}({self._data})'

    def __str__(self):
        return str(self._data)
