from lightning_flow import Task, Job, Workflow
from lightning_flow.parameters import *
from lightning_flow.funcs import output


class ExampleJob(Job):

    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(name, **kwargs)
        self.tasks = [
            Task('Output p1', self, callback=self.output_p1),
            Task('Output p2', self, callback=self.output_p2),
        ]

    p1 = IntParameter(min=1, max=5)
    p2 = OptionsParameter(options=['a', 'b', 'c', 'd', 'e'])

    def output_p1(self, context):
        context.data['p1'] = self.p1
        output(context.data)

    def output_p2(self, context):
        context.data['p2'] = self.p2
        output(context.data)


class ExampleWorkflow(Workflow):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(name, **kwargs)
        job1 = ExampleJob(name='job1', p1=1, p2='a')
        job2 = ExampleJob(name='job2', p1=2, p2='b')
        job3 = ExampleJob(name='job3', p1=3, p2='c')
        job4 = ExampleJob(name='job4', p1=4, p2='d')
        job5 = ExampleJob(name='job5', p1=5, p2='e')
        job2['Output p1'].skip = True
        job4['Output p2'].skip = True
        self.jobs = [
            job1,
            job2, 
            job3,
            job4,
            job5,
        ]
        self.dependence = {
            job1[0]: [job2[0], job3[0]],
            job2[1]: job1[1],
            job3[1]: job1[1],
        }

wf = ExampleWorkflow('wf1')
wf.run()
