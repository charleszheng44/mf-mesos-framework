import os
import sys
import time
import urllib
import logging

from mesos.interface import Scheduler
from mesos.native import MesosSchedulerDriver
from mesos.interface import mesos_pb2

logging.basicConfig(level=logging.INFO)

FILE_RUN_TASKS = "task_to_run"
FILE_FINISH_TASKS = "finished_tasks"
MF_DONE_FILE = "done"

def make_mf_mesos_executor(mf_task):
    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = str(mf_task.task_id)
    sh_path = os.path.abspath("./mf-mesos-executor.in")
    executor.name = "{} makeflow mesos executor".format(mf_task.task_id) 
    executor.source = "python executor"
    executor.command.value = "{} \"{}\" {}".format(sh_path, mf_task.cmd, executor.executor_id.value)
    for fn in mf_task.inp_fns:
        uri = executor.command.uris.add()
        logging.info("input file is: {}".format(fn.strip(' \t\n\r')))
        uri.value = fn.strip(' \t\n\r')
        uri.executable = False
        uri.extract = False
    return executor

def new_task(offer, task_id):
    task = mesos_pb2.TaskInfo()
    task.task_id.value = task_id
    task.slave_id.value = offer.slave_id.value
    task.name = "task {}".format(str(id))

    cpus = task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = 1

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = 1

    return task

def get_new_task(task_list):
    inp_fn = open(FILE_RUN_TASKS, "r")
    lines = inp_fn.readlines()
    processing = False
    new_task = False

    for line in lines:
        if line.strip(' \t\n\r') == "done":
            return ("done", None)

        words = line.split(':')
        # find new task
        if processing:
            if words[0] == "cmd":
                cmd = words[1].strip(' \t\n\r')
            if words[0] == "input files":
                inp_fns = words[1].strip(' \t\n\r').split(',')
                inp_fns.pop()
            if words[0] == "output files":
                oup_fns = words[1].strip(' \t\n\r').split(',')
                oup_fns.pop()
                processing = False
                break

        if (words[0] == "task_id") and (words[1].strip(' \t\n\r') not in task_list):
            task_id = words[1].strip(' \t\n\r')
            processing = True
            new_task = True
                
    inp_fn.close()

    if new_task:
        mf_task = MakeflowTask(task_id, cmd, inp_fns, oup_fns)
        return ("working", mf_task)
    else: 
        return ("working", None)

# Makeflow task class
class MakeflowTask:

    def __init__(self, task_id, cmd, inp_fns, oup_fns):
        self.task_id = task_id
        self.cmd = cmd
        self.inp_fns = inp_fns
        self.oup_fns = oup_fns

class MakeflowScheduler(Scheduler):

    def __init__(self, mf_wk_dir):
        self.task_list = {}
        self.mf_wk_dir = mf_wk_dir

    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: {}".format(framework_id))

    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: {}".format([o.id.value for o in offers]))
        

        for offer in offers:

            state_and_task = get_new_task(self.task_list)            
                
            if state_and_task[1] != None:
                # use the offer if there is new task
                mf_task = state_and_task[1]
                task = new_task(offer, mf_task.task_id)
                executor = make_mf_mesos_executor(mf_task)
                task.executor.MergeFrom(executor)
                
                time.sleep(2)
                logging.info("Launching task {task} "
                             "using offer {offer}.".format(task=task.task_id.value,
                                                        offer=offer.id.value))
                tasks = [task]
                self.task_list[mf_task.task_id] = mf_task
                driver.launchTasks(offer.id, tasks)
            else:
                # decline the offer, if there is no new task
                driver.declineOffer(offer.id)

        mf_done_fn_path = os.path.join(self.mf_wk_dir, MF_DONE_FILE)

        if os.path.isfile(mf_done_fn_path):
            fn_run_tks_path = os.path.join(self.mf_wk_dir, FILE_RUN_TASKS)
            fn_finish_tks_path = os.path.join(self.mf_wk_dir, FILE_FINISH_TASKS)
        
            if os.path.isfile(fn_run_tks_path):
                os.remove(fn_run_tks_path)
            if os.path.isfile(fn_finish_tks_path):
                os.remove(fn_finish_tks_path)

            driver.stop()

    def statusUpdate(self, driver, update):
        print "Task {} is in state {}".format(update.task_id.value, update.state)

        if os.path.isfile(FILE_FINISH_TASKS): 
            oup_fn = open(FILE_FINISH_TASKS, "a", 0)
        else:
            oup_fn = open(FILE_FINISH_TASKS, "w", 0)

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
            output_fns = self.task_list[curr_task_id].oup_fns

            print "+++++++++++"
            print output_fns
            print "+++++++++++"

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
