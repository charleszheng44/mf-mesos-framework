import os
import sys
import uuid
import time
import urllib
import logging
import threading
import mf_mesos_setting

from mesos.interface import Scheduler
from mesos.native import MesosSchedulerDriver
from mesos.interface import mesos_pb2

logging.basicConfig(level=logging.INFO)

FILE_TASK_INFO = "task_info"
FILE_TASK_STATE = "task_state"
MF_DONE_FILE = "makeflow_done"

# Create a ExecutorInfo instance for mesos task
def new_mesos_executor(mf_task, framework_id):
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
def new_mesos_task(offer, task_id):
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


# stop all running executors
def stop_executors(driver, tasks_info_dict):
    task_action_fn = open(FILE_TASK_INFO, "r")
    lines = task_action_fn.readlines()
    for line in lines:
        task_info_list = line.split(",")
        task_id = task_info_list[0]
        task_action = task_info_list[4]
        if task_action == "aborting":
            mf_task = tasks_info_dict[task_id]
            driver.sendFrameworkMessage(self, mf_task.executor_id, mf_task.slave_id, "abort")
    task_action_fn.close()

# Check if all tasks done
def is_all_executor_stopped(executors_info_dict):
    for executor_info in executors_info_dict.itervalues():
        if executor_info.state == "registered":
            return False
    return True

# Makeflow mesos scheduler
class MakeflowScheduler(Scheduler):

    def __init__(self, mf_wk_dir):
        self.mf_wk_dir = mf_wk_dir

    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: {}".format(framework_id))

    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: {}".format([o.id.value for o in offers]))
        
        for offer in offers:

            # Put offer into the offers_queue
            mf_mesos_setting.offers_queue.put(offer)
        
    def statusUpdate(self, driver, update):

        if os.path.isfile(FILE_TASK_STATE): 
            oup_fn = open(FILE_TASK_STATE, "a", 0)
        else:
            logging.error("{} is not created in advanced".format(FILE_TASK_STATE))

        if update.state == mesos_pb2.TASK_FAILED:
            oup_fn.write("{},failed\n".format(update.task_id.value))
        if update.state == mesos_pb2.TASK_FINISHED:
            oup_fn.write("{},finished\n".format(update.task_id.value))

        mf_mesos_setting.tasks_info_dict[update.task_id.value].action = "done"
        oup_fn.close()

    def frameworkMessage(self, driver, executorId, slaveId, message):
        logging.info("Receive message {}".format(message))
        message_list = message.split()

        if message_list[0].strip(' \t\n\r') == "output_file_dir":
            output_file_dir = message_list[1].strip(' \t\n\r')
            curr_task_id = message_list[3].strip(' \t\n\r')
            output_fns = mf_mesos_setting.tasks_info_dict[curr_task_id].oup_fns

            for output_fn in output_fns:
                output_file_addr = "{}/{}".format(output_file_dir, output_fn)
                logging.info("The output file address is: {}".format(output_file_addr))
                urllib.urlretrieve(output_file_addr, output_fn)
        
        if message_list[0].strip(' \t\n\r') == "[EXUT_STATE]":
            curr_executor_id = message_list[1].strip(' \t\n\r')
            curr_executor_state = message_list[2].strip(' \t\n\r')
            mf_mesos_setting.executors_info_dict[curr_executor_id].state = curr_executor_state

class MakefowMonitor(threading.Thread):
  
    def __init__(self, driver, created_time):
        threading.Thread.__init__(self)
        self.last_mod_time = created_time
        self.driver = driver

    def stop_mesos_scheduler(self):

        # If makeflow creat "makeflow_done" file, stop the scheduler
        mf_done_fn_path = os.path.join(mf_mesos_setting.mf_wk_dir, MF_DONE_FILE)

        if os.path.isfile(mf_done_fn_path):
            mf_done_fn = open(mf_done_fn_path, "r")
            mf_state = mf_done_fn.readline().strip(' \t\n\r')
            mf_done_fn.close()

            logging.info("Makeflow workflow is {}".format(mf_state))

            if mf_state == "aborted":
                logging.info("Workflow aborted, stopping executors...")
                stop_executors(self.driver, mf_mesos_setting.tasks_info_dict)

            fn_run_tks_path = os.path.join(mf_mesos_setting.mf_wk_dir, FILE_TASK_INFO)
            fn_finish_tks_path = os.path.join(mf_mesos_setting.mf_wk_dir, FILE_TASK_STATE)

            #if os.path.isfile(mf_done_fn_path):
            #    os.remove(mf_done_fn_path)
            #if os.path.isfile(fn_run_tks_path):
            #    os.remove(fn_run_tks_path)
            #if os.path.isfile(fn_finish_tks_path):
            #    os.remove(fn_finish_tks_path)
           
            while(not is_all_executor_stopped(mf_mesos_setting.executors_info_dict)):
                pass

            self.driver.stop()  

    def launch_mesos_task(self, task_id, task_cmd, task_inp_fns, task_oup_fns, \
            task_action): 

        while(mf_mesos_setting.offers_queue.empty()):
                logging.info("Waiting for available
                        offer.")
                time.sleep(2)

        offer = mf_mesos_setting.offers_queue.get()

        mf_mesos_task_info = mf_mesos_setting.MfMesosTaskInfo(\
                                task_id, task_cmd, task_inp_fns, task_oup_fns, \
                                task_action)

        mesos_task = new_mesos_task(offer, task_id)

        while(not mf_mesos_setting.lock.acquire()):
            pass

        logging.info("Found a new task and launch it on 
                mesos.")
        
        executor = new_mesos_executor(\
            mf_mesos_task_info, \
            offer.framework_id.value)

        mesos_task.executor.MergeFrom(executor)

        mf_mesos_executor_info = \
                mf_mesos_setting.MfMesosExecutorInfo(\
                executor.executor_id, \
                offer.slave_id.value, offer.hostname) 

        mf_mesos_setting.executors_info_dict[excutor.executor_id] = \
                mf_mesos_executor_info

        mf_mesos_task_info.executor_info = \
                mf_mesos_executor_info

        mf_mesos_setting.tasks_info_dict[task_id] \
                = mf_mesos_task_info 
        
        # create mesos task and launch it with 
        # offer 
        logging.info("Launching task {} 
                using offer {}.".format(\
                        task_id, offer.id.value))

        # one task is corresponding to one executor
        tasks = [mesos_task]
        self.driver.launchTasks(offer.id, tasks)

        mf_mesos_setting.lock.release()
    
    def abort_mesos_task(self):
        logging.info("Makeflow is trying to abort task {}
                                ".format(task_id))

        while(not mf_mesos_setting.lock.acquire()):
            pass

        mf_mesos_setting.tasks_info_dict[task_id].action\
                = "aborting"

        abort_executor_id = \
                mf_mesos_setting.tasks_info_dict[task_id].\
                executor_info.executor_id

        abort_slave_id = \
                mf_mesos_setting.tasks_info_dict[task_id].\
                executor_info.slave_id
        
        mf_mesos_setting.lock.release()

        self.driver.sendFrameworkMessage(executor_id, slave_id,\
                "[SCH_REQUEST] abort")


    def run(self):

        while(!os.path.isfile(MF_DONE_FILE)):

            mod_time = os.stat(FILE_TASK_INFO).st_mtime

            if (self.last_mod_time != mod_time):
                logging.info("{} is modified at {}".format(\
                        FILE_TASK_INFO, mod_time))
                self.last_mod_time = mod_time
                task_info_fp = open(FILE_TASK_INFO, "r")
                lines = task_info_fp.readlines()

                for line in lines:
                    task_info_list = line.split(",")
                    task_id = task_info_list[0]
                    task_cmd = task_info_list[1]
                    task_inp_fns = task_info_list[2].split()
                    task_oup_fns = task_info_list[3].split()
                    task_action = task_info_list[4]
                    
                    # find new tasks
                    if (task_id not in mf_mesos_setting.tasks_info_dict):

                        launch_mesos_task(task_id, task_cmd, task_inp_fns, \
                                task_oup_fns, task_action)
                        
                    # makeflow trying to abort an exist task
                    if (task_id in mf_mesos_setting.tasks_info_dict) and\
                            (task_action = "aborting"):

                        abort_mesos_task(task_id)                

            else:
                time.sleep(1)
       
        stop_mesos_scheduler() 

if __name__ == '__main__':
    # make us a framework
    mf_mesos_setting.mf_wk_dir = sys.argv[1]

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
