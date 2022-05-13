from enum import IntEnum
from typing import Callable

from pyparsing import Optional

from .task_data import TaskData
from ..import utils, funcs

class TaskState(IntEnum):
    """Flag the current state of the task"""
    PENDING = 0
    INPROGRESS = 1
    SUCCESS = 2
    FAILURE = 3
    SKIPPED = 4

class Task:
    """The atomic executing unit."""
    def __init__(self, name: str, *, callback: Callable, setup: Optional[Callable] = None, teardown: Optional[Callable] = None) -> None:
        """
        Args:
            name: The name of the task.
            callback: 
            setup: A callable called before the task callable function is run. If error occurs, the callable function will not be run.
            teardown: A callable that is always called at the end of the task, regardless whether the setup and callback complete without exception.
        """
        self._name = name
        self._callback = callback
        self._setup = setup
        self._teardown = teardown
        self._skip = False
        self._state = TaskState.PENDING
        self._err_info = None

    @property
    def name(self) -> str:
        """The name of the task."""
        return self._name

    @property
    def state(self) -> TaskState:
        """The state of the task."""
        return self._state

    @property
    def skip(self) -> bool:
        """A flag whether this task should be skipped."""
        return self._skip

    @skip.setter
    def skip(self, value: bool) -> None:
        self._skip = bool(value)

    @property
    def is_pending(self) -> bool:
        return self.state == TaskState.PENDING

    @property
    def is_inprogress(self) -> bool:
        return self.state == TaskState.INPROGRESS

    @property
    def is_completed(self) -> bool:
        return self.state > TaskState.INPROGRESS

    @property
    def err_info(self) -> Optional[str]:
        """Returns the error information if error occurred during task execution."""
        return self._err_info

    def run(self, data, context):
        if data is None:
            data = TaskData()
        if not self.is_pending():
            raise ValueError(f'Only a task in PENDING state can be run. Current state: {self.state.name}')
        if self.skip:
            self._state = TaskState.SKIPPED
        else:
            self._state = TaskState.INPROGRESS
            try:
                try:
                    if self._setup is not None:
                        self._setup(data, context)

                    self._callback()
                finally:
                    if self._teardown is not None:
                        self._teardown(data, context)
            except:
                self._err_info = utils.get_tb_str()
                self._state = TaskState.FAILURE
                funcs.output(self.err_info, level='error')
            else:
                self._state = TaskState.SUCCESS

        data.add_task_history(self.name)
        return data # TODO
