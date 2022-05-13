class Workflow(ABC):
    """The base class for all Workflows. An workflow class should inherit 
    from this class, and override the `run` method.
    """

    def __init__(self, name: str) -> None:
        """
        Args:
            name: The name of the workflow.
        """
        if not isinstance(name, str):
            raise TypeError(f"Type {type(name).__name__} is not valid for parameter name. Must be type str.")
        if not name:
            raise ValueError('Empty name str.')
        self.__name = name
        self.__status = RunningStatus.PENDING
        self.__err_info = None
        self.__thread = None

    @property
    def name(self) -> str:
        return self.__name
    
    @property
    def status(self) -> RunningStatus:
        return self.__status

    @property
    def err_info(self):
        return self.__err_info

    @property
    def _thread(self):
        return self.__thread

    @abstractmethod
    def run(self) -> None:
        """This method must be overridden."""
    
    def _lifecycle(self) -> None:
        self.__status = RunningStatus.INPROGRESS
        try:
            self.run()
        except:
            self.__status = RunningStatus.FAILURE
            self.__err_info = get_tb_str()
            output(self.err_info, level='error')
        else:
            self.__status = RunningStatus.SUCCESS

    def start(self) -> None:
        self.__thread = Thread(name=f'Thread-{self.name}', target=self._lifecycle)
        self._thread.start()