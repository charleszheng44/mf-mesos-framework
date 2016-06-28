import os
import sys
import json
import urllib2
import threading
import subprocess

from mesos.native import MesosExecutorDriver
from mesos.interface import Executor
from mesos.interface import mesos_pb2

DEFAULT_SLAVE_IP = "http://localhost:5051"

def get_sandbox_dir(executor_id, task_id):
    slave_state_uri = "{}/state.json".format(DEFAULT_SLAVE_IP)
    slave_state = json.load(urllib2.urlopen(slave_state_uri))
    executors_data = slave_state['completed_frameworks'][0]['completed_executors']
    for executor_data in executors_data:
        if executor_data['completed_tasks'][0]['executor_id'] == executor_id:
            return executor_data['directory']

class MakeflowMesosExecutor(Executor):

    def __init__(self, cmd, executor_id):
        self.cmd = cmd
        self.executor_id = executor_id

    def launchTask(self, driver, task):
        def run_task():
            print "Running task %s" % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            # Launch the makeflow task
            rec = subprocess.check_call(self.cmd, shell=True)

            print "Sending status update..."
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED

            driver.sendStatusUpdate(update)
            print "Sent status update"

            # send the sandbox URI to the scheduler
            sandbox_dir = get_sandbox_dir(self.executor_id, task.task_id.value)     
            get_dir_addr = "output_file_dir http://localhost:5051/files/download?path={}".format(sandbox_dir)
            task_id_msg = "task_id {}".format(task.task_id.value)
            message = "{} {}".format(get_dir_addr, task_id_msg) 
            print "Sending message: {}".format(message)
            driver.sendFrameworkMessage(message)
            print "Sent output file URI" 

        thread = threading.Thread(target=run_task)
        thread.start()
        thread.join()

if __name__ == '__main__':
    print "starting makeflow mesos executor!"
    driver = MesosExecutorDriver(MakeflowMesosExecutor(sys.argv[1], sys.argv[2]))
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
