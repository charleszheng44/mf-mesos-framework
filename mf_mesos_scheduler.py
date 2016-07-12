import os
import sys
import uuid
import time
import urllib
import logging

import threading

from mesos.interface import Scheduler
from mesos.native import MesosSchedulerDriver
from mesos.interface import mesos_pb2

logging.basicConfig(level=logging.INFO)

FILE_TASK_INFO = "task_info"
FILE_TASK_STATE = "task_state"
MF_DONE_FILE = "makeflow_done"

# Create a ExecutorInfo instance for mesos task
def make_mf_mesos_executor(mf_task, framework_id):
    executor = mesos_pb2.ExecutorInfo()
    executor.framework_id.value = framework_id
    executor.executor_id.value = str(uuid.uuid4())
    sh_path = os.path.abspath("./mf-mesos-executor.in")
    executor.name = "{} makeflow mesos executor".format(mf_task.task_id) 
    executor.source = "python executor"
    executor.command.value = "{} \"{}\" {} {}".format(sh_path, mf_task.cmd, 
            executor.executor_id.value, executor.framework_id.value)
    for fn in mf_task.inp_fns:
        uri = executor.command.uris.add()
        logging.info("input file is: {}".format(fn.strip(' \t\n\r')))
        uri.value = fn.strip(' \t\n\r')
        uri.executable = False
        uri.extract = False
    return executor

# Create a TaskInfo instance
def new_task(offer, task_id):
    mesos_task = mesos_pb2.TaskInfo()
    mesos_task.task_id.value = task_id
    mesos_task.slave_id.value = offer.slave_id.value
    mesos_task.name = "task {}".format(str(id))

    cpus = mesos_task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = 1

    mem = mesos_task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = 1

    return mesos_task

# If there is new task submited by makeflow, get it 
def get_new_task(tasks_table):
    task_action_fn = open(FILE_TASK_INFO, "r")
    lines = task_action_fn.readlines()
    mf_task = None
    for line in lines:
        task_info_list = line.split(",")
        task_id = task_info_list[0]
        task_cmd = task_info_list[1]
        task_inp_fns = task_info_list[2].split()
        task_oup_fns = task_info_list[3].split()
        task_action = task_info_list[4]

        # If there is a task submitted
        if (task_id not in tasks_table):
            mf_task = MakeflowTask(task_id, task_cmd, task_inp_fns, task_oup_fns, task_action)
            task_action_fn.close()
            break;
        
    task_action_fn.close()

    return mf_task

# stop all running executors
def stop_executors(driver, tasks_table):
    task_action_fn = open(FILE_TASK_INFO, "r")
    lines = task_action_fn.readlines()
    for line in lines:
        task_info_list = line.split(",")
        task_id = task_info_list[0]
        task_action = task_info_list[4]
        if task_action == "aborting":
            mf_task = tasks_table[task_id]
            driver.sendFrameworkMessage(self, mf_task.executor_id, mf_task.slave_id, "abort")
    task_action_fn.close()

# Check if all tasks done
def is_all_executor_stopped(executors_state):
    for executor_state in executors_state.itervalues():
        if executor_state == "registered":
            return False
    return True

# Makeflow task class
class MakeflowTask:

    def __init__(self, task_id, cmd, inp_fns, oup_fns, action):
        self.task_id = task_id
        self.cmd = cmd
        self.inp_fns = inp_fns
        self.oup_fns = oup_fns
        self.action = action

# Makeflow mesos scheduler
class MakeflowScheduler(Scheduler):

    def __init__(self, mf_wk_dir):
        self.tasks_table = {}
        self.executors_state = {}
        self.mf_wk_dir = mf_wk_dir

    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: {}".format(framework_id))

    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: {}".format([o.id.value for o in offers]))
        
        for offer in offers:

            mf_task = get_new_task(self.tasks_table)            
                
            if mf_task != None:
                # use the offer if there is new task
                mesos_task = new_task(offer, mf_task.task_id)
                executor = make_mf_mesos_executor(mf_task, offer.framework_id.value)
                mesos_task.executor.MergeFrom(executor)
             
                mf_task.executor_id = executor.executor_id
                mf_task.slave_id = offer.slave_id.value
                mf_task.hostname = offer.hostname
                
                time.sleep(2)
                logging.info("Launching task {task} "
                             "using offer {offer}.".format(task=mesos_task.task_id.value,offer=offer.id.value))
                tasks = [mesos_task]
                self.tasks_table[mf_task.task_id] = mf_task
                driver.launchTasks(offer.id, tasks)

            else:
                # decline the offer, if there is no new task
                driver.declineOffer(offer.id)

        # If makeflow creat "makeflow_done" file, stop the scheduler
        mf_done_fn_path = os.path.join(self.mf_wk_dir, MF_DONE_FILE)

        if os.path.isfile(mf_done_fn_path):
            mf_done_fn = open(mf_done_fn_path, "r")
            mf_state = mf_done_fn.readline().strip(' \t\n\r')
            mf_done_fn.close()

            logging.info("Makeflow workflow is {}".format(mf_state))

            if mf_state == "aborted":
                logging.info("Workflow aborted, stopping executors...")
                stop_executors(driver, self.tasks_table)

            fn_run_tks_path = os.path.join(self.mf_wk_dir, FILE_TASK_INFO)
            fn_finish_tks_path = os.path.join(self.mf_wk_dir, FILE_TASK_STATE)

            #if os.path.isfile(mf_done_fn_path):
            #    os.remove(mf_done_fn_path)
            #if os.path.isfile(fn_run_tks_path):
            #    os.remove(fn_run_tks_path)
            #if os.path.isfile(fn_finish_tks_path):
            #    os.remove(fn_finish_tks_path)
           
            while(not is_all_executor_stopped(self.executors_state)):
                pass

            driver.stop()

    def statusUpdate(self, driver, update):

        if os.path.isfile(FILE_TASK_STATE): 
            oup_fn = open(FILE_TASK_STATE, "a", 0)
        else:
            logging.error("{} is not created in advanced".format(FILE_TASK_STATE))

        if update.state == mesos_pb2.TASK_FAILED:
            oup_fn.write("{},failed\n".format(update.task_id.value))
        if update.state == mesos_pb2.TASK_FINISHED:
            oup_fn.write("{},finished\n".format(update.task_id.value))

        self.tasks_table[update.task_id.value].action = "done"
        oup_fn.close()

    def frameworkMessage(self, driver, executorId, slaveId, message):
        logging.info("Receive message {}".format(message))
        message_list = message.split()

        if message_list[0].strip(' \t\n\r') == "output_file_dir":
            output_file_dir = message_list[1].strip(' \t\n\r')
            curr_task_id = message_list[3].strip(' \t\n\r')
            output_fns = self.tasks_table[curr_task_id].oup_fns

            for output_fn in output_fns:
                output_file_addr = "{}/{}".format(output_file_dir, output_fn)
                logging.info("The output file address is: {}".format(output_file_addr))
                urllib.urlretrieve(output_file_addr, output_fn)
        
        if message_list[0].strip(' \t\n\r') == "[EXUT_STATE]":
            curr_executor_id = message_list[1].strip(' \t\n\r')
            curr_executor_state = message_list[2].strip(' \t\n\r')
            self.executors_state[curr_executor_id] = curr_executor_state


if __name__ == '__main__':
    # make us a framework
    mf_wk_dir = sys.argv[1]
    # just create the "task_state" file
    open(FILE_TASK_STATE, 'w').close()
    open(FILE_TASK_INFO, 'w').close()

    # initialize a framework instance
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "Makeflow"
    driver = MesosSchedulerDriver(
        MakeflowScheduler(mf_wk_dir),
        framework,
        "127.0.0.1:5050/"  # assumes running on the master
    )
    
    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    driver.stop()

    sys.exit(status)
