from prefect import task, Flow, Parameter, Task

class PrintTask(Task):
    def run(self, name):
        import ipdb; ipdb.set_trace()
        print(name)

with Flow("My First Flow") as flow:
    name = Parameter('name')
    a = PrintTask()
    a(name)

if __name__ == '__main__':
  flow.run(name="ECHO")
