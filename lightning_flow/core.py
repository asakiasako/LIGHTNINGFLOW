from __future__ import annotations
from typing import Optional, Iterable, Mapping, List, Callable
from enum import IntEnum
from collections import UserDict
from copy import deepcopy

import networkx

from .parameters import Parameter
from .exceptions import DirectedAcyclicGraphInvalid, DirectedAcyclicGraphUndefined
from .environment import Environment
from .import utils, funcs


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


class TaskContext:
    """ This class contains information about the context the task is running in. """

    def __init__(self, data: TaskData, task_name: str, job_name: str, workflow_name: str):
        """
        Args:
            data: The dataset passed between tasks.
            task_name: The name of the task.
            job_name: The name of the Job the task was started from.
            workflow_name: The name of the workflow the task was started from.
        """
        self.data = data
        self.task_name = task_name
        self.job_name = job_name
        self.workflow_name = workflow_name

    def to_dict(self):
        """ Return the task context content as a dictionary. """
        return {
            'data': self.data,
            'task_name': self.task_name,
            'job_name': self.job_name,
            'workflow_name': self.workflow_name
        }


class TaskState(IntEnum):
    """Flag the current state of the task"""
    PENDING = 0
    # Reserved for PAUSED = 1
    INPROGRESS = 2
    SUCCESS = 3
    FAILURE = 4
    SKIPPED = 5


class Task:
    """The atomic executing unit."""
    def __init__(
        self, 
        name: str, 
        parent: Job,
        *, 
        callback: Callable, 
        setup: Optional[Callable] = None, 
        teardown: Optional[Callable] = None
    ) -> None:
        """
        Args:
            name: The name of the task.
            parent: The job which contains this task.
            callback: A reference to the callable that should be called by the 
                task.
            setup: An optional callable called before the task callback 
                function runs. If an exception is raised, the callback 
                function will never run.
            teardown: A callable that is always called at the end of the 
                task, regardless whether the setup and callback completed 
                successfully or failed.

        The callback, setup and teardown callables should have the following 
        definition:

        ``` python
        (context) -> TaskContext
        ```

        context is the TaskContext object passed to the callables. The 
        context will be returned after the task is completed.
        """
        self._name = name
        self._parent = parent
        self._callback = callback
        self._setup = setup
        self._teardown = teardown
        self._skip = False
        self._state = TaskState.PENDING
        self._err_info = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}{''.join(f', {i}={getattr(self, i)!r}' for i in self.__parameter_names)})"

    @property
    def name(self) -> str:
        """The name of the task."""
        return self._name

    @property
    def parent(self) -> Job:
        """The job which contains this task."""
        return self._parent

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

    def run(self, context: TaskContext) -> TaskContext:
        """Run the task.
        
        Args:
            context: The context in which the task runs.

        Returns:
            The context after the task completed.
        """
        if not self.is_pending():
            raise ValueError(f'Only a task in PENDING state can be run. Current state: {self.state.name}')
        if self.skip:
            self._state = TaskState.SKIPPED
        else:
            self._state = TaskState.INPROGRESS
            try:
                try:
                    if self._setup is not None:
                        self._setup(context)

                    self._callback(context)
                finally:
                    if self._teardown is not None:
                        self._teardown(context)
            except:
                self._err_info = utils.get_tb_str()
                self._state = TaskState.FAILURE
            else:
                self._state = TaskState.SUCCESS
        context.data.add_task_history(self.name)
        return context


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
        return f"Job: {self.__class__.__name__}(name={self.name!r}{''.join(f', {i}={getattr(self, i)!r}' for i in self.__parameter_names)})"
        
    def __load_parameters(self, **kwargs) -> None:
        p_names = set(self.__parameter_names)
        arg_keys = set(kwargs.keys())
        unassigned = sorted(p_names - arg_keys)
        if unassigned:
            raise TypeError(f"Missing keyword arguments for these parameters: {', '.join(unassigned)}")
        for k, v in kwargs.items():
            if k in p_names:
                setattr(self, k, v)

    def __len__(self):
        return len(self.tasks)

    def __getitem__(self, idx):
        return self.tasks[idx]

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


class WorkflowState(IntEnum):
    """Flag the current state of the workflow"""
    PENDING = 0
    # Reserved for PAUSED = 1
    INPROGRESS = 2
    SUCCESS = 3
    FAILURE = 4
    # Reserved for SKIPPED = 5


class Workflow:
    """The base class for all Workflows. An workflow class should inherit 
    from this class, and override the `run` method.
    """

    def __init__(self, name: str, **kwargs) -> None:
        """
        Args:
            name: The name of the job.
            **kwargs: Passed to the parameters of the job, or ignored if the 
            corresponding parameter is not defined.
        """
        self._name = name
        self._jobs = []
        self._dependencies = None
        self._state = WorkflowState.PENDING
        self.__parameter_names = sorted(i for i in dir(self.__class__) 
            if isinstance(getattr(self.__class__, i), Parameter))
        self.__load_parameters(**kwargs)

    def __repr__(self) -> str:
        return f"Workflow: {self.__class__.__name__}(name={self.name!r}{''.join(f', {i}={getattr(self, i)!r}' for i in self.__parameter_names)})"

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
    def state(self):
        return self._state

    @property
    def jobs(self) -> List[Job]:
        return self._jobs

    @property
    def dependencies(self) -> Optional[dict]:
        return self._dependencies

    @dependencies.setter
    def dependencies(self, value: dict) -> None:
        self._dependencies = value

    def _validate_dag(self, graph: networkx.DiGraph) -> bool:
        if not networkx.is_directed_acyclic_graph(graph):
            raise DirectedAcyclicGraphInvalid

    def _make_graph(self):

        schema = {}

        for job in self.jobs:
            if not job:
                raise ValueError(f'Empty job without any tasks: {job!r}')
            for i in range(len(job)-1):
                schema[job[i]] = [job[i+1]]

        if self.dependencies:
            for k, v in self.dependencies.items():
                schema.setdefault(k, []).append(v)

        if schema is None:
            raise DirectedAcyclicGraphUndefined

        # sanitize the input schema such that it follows the structure:
        #    {parent: [child1, child2, ...], ...}
        sanitized_schema = {}
        for parent, children in schema.items():
            child_list = []
            if children is not None:
                if isinstance(children, list):
                    if len(children) > 0:
                        child_list = children
                    else:
                        child_list = [None]
                else:
                    child_list = [children]
            else:
                child_list = [None]

            sanitized_schema[parent] = child_list

        # build the graph from the sanitized schema
        graph = networkx.DiGraph()

        for parent, children in sanitized_schema.items():
            for child in children:
                if child is not None:
                    graph.add_edge(parent, child)
                else:
                    graph.add_node(parent)

        self._validate_dag(graph=graph)

        return graph

    def run(self):
        self._state = WorkflowState.INPROGRESS
        graph = self._make_graph()
        n_nodes = graph.number_of_nodes()
        lexicographic = [t for job in self.jobs for t in job]

        context = TaskContext(data=TaskData())
        env = Environment()
        idx = 0
        for task in networkx.lexicographical_topological_sort(graph, key=lambda x: lexicographic.index(x)):
            if task.state == TaskState.PENDING:
                idx += 1
                context.task_name = env.currentTask = task.name
                context.job_name = env.currentJob = task.parent.name
                context.workflow_name = env.currentWorkflow = self.name
                print(f"[Task {idx}/{n_nodes}] {self.name}/{task.parent.name}/{task.name}:")
                context = task.run(context=context)
                print(f"  - {task.state}")
                if task.state == TaskState.FAILURE:
                    self.state = WorkflowState.FAILURE
                    utils.output(task.err_info, level='error')
                    break
            else:
                raise ValueError(f'Task {task.name} of job {task.parent} is not in PENDING state. Current state: {task.state.name}')
        else:
            self.state = WorkflowState.SUCCESS
