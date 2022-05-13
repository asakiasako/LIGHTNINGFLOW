
class Job(ABC):

    def __init__(self, name: str, **kwargs) -> None:
        self.__name = name
        self.__tasks = []
        self.__tid = -1
        self.__status = RunningStatus.PENDING
        self.__parameter_names = tuple(sorted(i for i in dir(self.__class__) if isinstance(getattr(self.__class__, i), Parameter)))
        self.__load_parameters(**kwargs)
        self.__err_info = None

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.run(*args, **kwds)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}{''.join(f', {i}={getattr(self, i)!r}' for i in self.__parameter_names)})"
        
    def __load_parameters(self, **kwargs):
        p_names = set(self.__parameter_names)
        arg_keys = set(kwargs.keys())
        unassigned = sorted(p_names - arg_keys)
        if unassigned:
            raise TypeError(f"Missing keyword arguments for these parameters: {', '.join(unassigned)}")
        for k, v in kwargs.items():
            if k in p_names:
                setattr(self, k, v)

    @property
    def name(self):
        return self.__name

    @property
    def status(self):
        return self.__status

    @property
    def tasks(self):
        return tuple(self.__tasks)

    @property
    def tid(self):
        return self.__tid

    @tasks.setter
    def tasks(self, value):
        if self.status != RunningStatus.PENDING:
            raise RuntimeError('Appending tasks is only allowed before the job started running.')
        _tasks = list(value)
        if all(isinstance(i, Task) for i in _tasks):
            self.__tasks = _tasks
        else:
            raise TypeError('Property tasks must be a list of Task instances.')

    def append_task(self, task: Task):
        if self.status != RunningStatus.PENDING:
            raise RuntimeError('Appending tasks is only allowed before the job started running.')
        if not isinstance(task, Task):
            raise TypeError('Parameter task must be type Task.')
        self.__tasks.append(task)

    def run(self, await_on: Optional[Task | str | int] = None) -> None:
        if self.status not in { RunningStatus.PENDING, RunningStatus.INPROGRESS }:
            raise ValueError(f'A job in {self.status} could not run')
        self.__status = RunningStatus.INPROGRESS
        try:
            while self.tid < len(self.tasks):
                self.__tid += 1
                task = self.tasks[self.tid]
                task()
                if await_on == task or await_on == task.name or await_on == self.tid:
                    break
            else:
                self.__status = RunningStatus.SUCCESS
        except:
            self.__err_info = get_tb_str()
            self.__status = RunningStatus.FAILURE
            output(self.err_info, level='error')
        else:
            if self.tid >= len(self.tasks) - 1:
                self.__status = RunningStatus.SUCCESS
            else:
                self.__status = RunningStatus.PAUSED
