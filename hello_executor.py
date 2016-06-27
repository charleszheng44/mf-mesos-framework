import os
import sys
import threading

from mesos.native import MesosExecutorDriver
from mesos.interface import Executor
from mesos.interface import mesos_pb2

class HelloWorldExecutor(Executor):
    def launchTask(self, driver, task):
        def run_task():
            print "Running task %s" % task.task_id.value

        thread = threading.Thread(target=run_task)
        thread.start()

if __name__ == '__main__':
    print "starting hello_executor!"
    driver = MesosExecutorDriver(HelloWorldExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
