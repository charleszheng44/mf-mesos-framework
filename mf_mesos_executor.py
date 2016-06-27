import os
import sys
import threading
import subprocess

from mesos.native import MesosExecutorDriver
from mesos.interface import Executor
from mesos.interface import mesos_pb2

class MakeflowMesosExecutor(Executor):

    def __init__(self, cmd):
        self.cmd = cmd

    def launchTask(self, driver, task):
        def run_task():
            print "Running task %s" % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)
            # This is where one would perform the requested task.
            rec = subprocess.check_call(self.cmd, shell=True)

            print "Sending status update..."
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED

            driver.sendStatusUpdate(update)
            print "Sent status update"

        thread = threading.Thread(target=run_task)
        thread.start()
        thread.join()

if __name__ == '__main__':
    print "starting makeflow mesos executor!"
    driver = MesosExecutorDriver(MakeflowMesosExecutor(sys.argv[1]))
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
