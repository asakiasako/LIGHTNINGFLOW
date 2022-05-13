from enum import IntEnum
from typing import List

from .task import Task, TaskState
from .parameter import Parameter


class JobState(IntEnum):
    """Flag the current state of the job"""
    PENDING = 0
    PAUSED = 1
    INPROGRESS = 2
    SUCCESS = 3
    FAILURE = 4
    SKIPPED = 5


class Job:
    """A job consists of a sequence of tasks to complete a specific piece of 
    work.
    """

    def __init__(self, name: str, **kwargs) -> None:
        """
        Args:
            name: The name of the job.
            **kwargs: Passed to the parameters of the job, or ignored if the 
            corresponding parameter is not defined.
        """
        self._name = name
        self._tasks = []
        self._err_info = None
        self.__parameter_names = sorted(i for i in dir(self.__class__) 
            if isinstance(getattr(self.__class__, i), Parameter))
        self.__load_parameters(**kwargs)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}{''.join(f', {i}={getattr(self, i)!r}' for i in self.__parameter_names)})"
        
    def __load_parameters(self, **kwargs) -> None:
        p_names = set(self.__parameter_names)
        arg_keys = set(kwargs.keys())
        unassigned = sorted(p_names - arg_keys)
        if unassigned:
            raise TypeError(f"Missing keyword arguments for these parameters: {', '.join(unassigned)}")
        for k, v in kwargs.items():
            if k in p_names:
                setattr(self, k, v)

    @property
    def name(self) -> str:
        """The name of the job."""
        return self._name

    @property
    def state(self) -> JobState:
        """The state of the job."""
        task_states = {task.state for task in self.tasks}
        if not task_states or {TaskState.PENDING} == task_states:
            return JobState.PENDING
        if TaskState.INPROGRESS in task_states:
            return JobState.INPROGRESS
        if TaskState.FAILURE in task_states:
            return JobState.FAILURE
        if {TaskState.SKIPPED} == task_states:
            return JobState.SKIPPED
        if TaskState.PENDING in task_states:
            return JobState.PAUSED
        else:
            return JobState.SUCCESS

    @property
    def tasks(self) -> List[Task]:
        """The task sequence of the job."""
        return self._tasks

    def skip_all(self) -> None:
        """Set the `skip` flag of all tasks."""
        for t in self.tasks:
            t.skip = True
