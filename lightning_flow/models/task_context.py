from .task_data import TaskData


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