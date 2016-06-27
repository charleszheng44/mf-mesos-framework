import logging
import uuid
import time
import sys
import os

from mesos.interface import Scheduler
from mesos.native import MesosSchedulerDriver
from mesos.interface import mesos_pb2

logging.basicConfig(level=logging.INFO)

TOTAL_TASKS = 5
task_counter = 0

def make_hello_executor(task_id, curr_cmd):
    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = task_id
    sh_path = os.path.abspath("./test-executor.in")
    executor.command.value = "{} {}".format(sh_path, curr_cmd)
    executor.name = "{} hello executor".format(task_id) 
    executor.source = "python executor"

    return executor

class HelloWorldScheduler(Scheduler):

    def __init__(self):
        self.launched_tasks = 0

    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: {}".format(framework_id))

    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: {}".format([o.id.value for o in offers]))
        # whenever we get an offer, we accept it and use it to launch a task that
        # just echos hello world to stdout
        for offer in offers:
            if self.launched_tasks <= TOTAL_TASKS:

                self.launched_tasks = self.launched_tasks + 1
                
                task = mesos_pb2.TaskInfo()
                task.task_id.value = str(self.launched_tasks)
                task.slave_id.value = offer.slave_id.value
                task.name = "task {}".format(str(self.launched_tasks))
                curr_cmd = "\"Hello task {}\"".format(task.task_id.value)
                executor = make_hello_executor(task.task_id.value, curr_cmd) 
                task.executor.MergeFrom(executor)

                cpus = task.resources.add()
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = 1

                mem = task.resources.add()
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = 1

                time.sleep(2)
                logging.info("Launching task {task} "
                             "using offer {offer}.".format(task=task.task_id.value,
                                                           offer=offer.id.value))
                tasks = [task]

                driver.launchTasks(offer.id, tasks)

            else: 

                driver.stop()

if __name__ == '__main__':

    # make us a framework
    framework = mesos_pb2.FrameworkInfo()
    framework.user = "zc"  # Have Mesos fill in the current user.
    framework.name = "hello-world"
    framework.checkpoint = True

    driver = MesosSchedulerDriver(
        HelloWorldScheduler(),
        framework,
        "127.0.0.1:5050/"  # assumes running on the master
    )

    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
