import os
import sys
import time
import urllib
import logging

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
    executor.executor_id.value = str(mf_task.task_id)
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
def get_new_task(task_table):
    task_action_fn = open(FILE_TASK_INFO, "r")
    lines = task_action_fn.readlines()
    mf_task = None
    for line in lines:
        task_info_list = line.split(",")
        task_id = task_info_list[0]
        if (task_id not in task_table):
            task_cmd = task_info_list[1]
            task_inp_fns = task_info_list[2].split()
            task_oup_fns = task_info_list[3].split()
            task_action = task_info_list[4]
            mf_task = MakeflowTask(task_id, task_cmd, task_inp_fns, task_oup_fns, task_action)
            break;

    task_action_fn.close()
    return mf_task

# Makeflow task class
class MakeflowTask:

    def __init__(self, task_id, cmd, inp_fns, oup_fns, action):
        self.task_id = task_id
        self.cmd = cmd
        self.inp_fns = inp_fns
        self.oup_fns = oup_fns
        self.action = action

class MakeflowScheduler(Scheduler):

    def __init__(self, mf_wk_dir):
        self.task_table = {}
        self.mf_wk_dir = mf_wk_dir

    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: {}".format(framework_id))

    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: {}".format([o.id.value for o in offers]))
        

        for offer in offers:

            mf_task = get_new_task(self.task_table)            
                
            if mf_task != None:
                # use the offer if there is new task
                mesos_task = new_task(offer, mf_task.task_id)
                executor = make_mf_mesos_executor(mf_task, offer.framework_id.value)
                mesos_task.executor.MergeFrom(executor)
                
                time.sleep(2)
                logging.info("Launching task {task} "
                             "using offer {offer}.".format(task=mesos_task.task_id.value,
                                                        offer=offer.id.value))
                tasks = [mesos_task]
                self.task_table[mf_task.task_id] = mf_task
                driver.launchTasks(offer.id, tasks)
            else:
                # decline the offer, if there is no new task
                driver.declineOffer(offer.id)

        # If makeflow creat "makeflow_done" file, stop the scheduler
        mf_done_fn_path = os.path.join(self.mf_wk_dir, MF_DONE_FILE)

        if os.path.isfile(mf_done_fn_path):
            fn_run_tks_path = os.path.join(self.mf_wk_dir, FILE_TASK_INFO)
            fn_finish_tks_path = os.path.join(self.mf_wk_dir, FILE_TASK_STATE)
       
            os.remove(mf_done_fn_path)
            if os.path.isfile(fn_run_tks_path):
                os.remove(fn_run_tks_path)
            if os.path.isfile(fn_finish_tks_path):
                os.remove(fn_finish_tks_path)

            driver.stop()

    def statusUpdate(self, driver, update):
        print "Task {} is in state {}".format(update.task_id.value, update.state)

        if os.path.isfile(FILE_TASK_STATE): 
            oup_fn = open(FILE_TASK_STATE, "a", 0)
        else:
            oup_fn = open(FILE_TASK_STATE, "w", 0)

        if update.state == mesos_pb2.TASK_FAILED:
            oup_fn.write("{} failed\n".format(update.task_id.value))
        if update.state == mesos_pb2.TASK_FINISHED:
            oup_fn.write("{} finished\n".format(update.task_id.value))

        oup_fn.close()

    def frameworkMessage(self, driver, executorId, slaveId, message):
        print "Receive message {}".format(message)
        message_list = message.split()

        if message_list[0].strip(' \t\n\r') == "output_file_dir":
            output_file_dir = message_list[1].strip(' \t\n\r')
            curr_task_id = message_list[3].strip(' \t\n\r')
            output_fns = self.task_table[curr_task_id].oup_fns

            for output_fn in output_fns:
                output_file_addr = "{}/{}".format(output_file_dir, output_fn)
                print "The output file address is: {}".format(output_file_addr)
                urllib.urlretrieve(output_file_addr, output_fn)

if __name__ == '__main__':
    # make us a framework
    mf_wk_dir = sys.argv[1]
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
