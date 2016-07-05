import os
import sys
import json
import urllib2
import logging
import threading
import subprocess

from mesos.native import MesosExecutorDriver
from mesos.interface import Executor
from mesos.interface import mesos_pb2

logging.basicConfig(filename=('{}.log'.format(sys.argv[2])), level=logging.INFO)

DEFAULT_SLAVE_IP = "http://localhost:5051"

def get_sandbox_dir(framework_id, executor_id, task_id):
    slave_state_uri = "{}/state.json".format(DEFAULT_SLAVE_IP)

    slave_state = json.load(urllib2.urlopen(slave_state_uri))
    executors_data = slave_state['frameworks'][0]['executors']

    for executor_data in executors_data:

        if executor_data['id'] == executor_id:
            # The task is in the completed_tasks lists
            completed_tasks = executor_data['completed_tasks']
            for completed_task in completed_tasks:
                if completed_task['id'] == task_id:
                    return executor_data['directory']
           
            # due to the network delay, the task is in the tasks lists
            tasks = executor_data['tasks']
            for task in tasks:
                if task['id'] == task_id:
                    return executor_data['directory']
            
            logging.error("Task {} does not appear in the tasks list\
                    of executor {}.".format(task_id, executor_id))

            return None 

class MakeflowMesosExecutor(Executor):

    def __init__(self, cmd, executor_id, framework_id):
        self.cmd = cmd
        self.executor_id = executor_id
        self.framework_id = framework_id

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
            if rec == 0:
                update.state = mesos_pb2.TASK_FINISHED
            else:
                update.state = mesos_pb2.TASK_FAILED

            driver.sendStatusUpdate(update)
            print "Sent status update"

            # send the sandbox URI to the scheduler
            sandbox_dir = get_sandbox_dir(self.framework_id, self.executor_id, task.task_id.value)
            get_dir_addr = "output_file_dir http://localhost:5051/files/download?path={}".format(sandbox_dir)
            task_id_msg = "task_id {}".format(task.task_id.value)
            message = "{} {}".format(get_dir_addr, task_id_msg) 
            logging.info("Sending message: {}".format(message))
            driver.sendFrameworkMessage(message)
            print "Sent output file URI" 

        thread = threading.Thread(target=run_task)
        thread.start()
        thread.join()

if __name__ == '__main__':
    print "starting makeflow mesos executor!"
    driver = MesosExecutorDriver(MakeflowMesosExecutor(sys.argv[1], sys.argv[2], sys.argv[3]))
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
