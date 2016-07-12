import os
import time
import threading
import mf_mesos_setting

FILE_TASK_INFO = "task_info"
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


class TaskInfoMonitor(threading.Thread):
  
    def __init__(self, driver, created_time):
        threading.Thread.__init__(self)
        self.last_mod_time = created_time
        self.driver = driver

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

                        mf_mesos_task_info = mf_mesos_setting.MfMesosTaskInfo(\
                                task_id, task_cmd, task_inp_fns, task_oup_fns, \
                                task_action)

                        mesos_task = new_mesos_task(offer, task_id)

                        while(1): 

                            if mf_mesos_setting.lock.acquire():

                                logging.info("Found a new task and launch it on 
                                        mesos.")
                                
                                while(mf_mesos_setting.offers_queue.empty()):
                                    logging.info("Waiting for available
                                            offer.")
                                    time.sleep(2)

                                offer = mf_mesos_setting.offers_queue.get()

                                executor = new_mesos_executor(\
                                        mf_mesos_task_info, \
                                        offer.framework_id.value)

                                mesos_task.executor.MergeFrom(executor)

                                mf_mesos_executor_info = \
                                        mf_mesos_setting.MfMesosExecutorInfo(\
                                        executor.executor_id, \
                                        offer.slave_id.value, offer.hostname) 

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
                                break

                            else:
                                logging.info("Found a new task and wait for the
                                        lock.")
                                time.sleep(2)

                    # makeflow trying to abort an exist task
                    if (task_id in mf_mesos_setting.tasks_info_dict) and\
                            (task_action = "aborting"):
                        logging.info("Makeflow is trying to abort task {}
                                ".format(task_id))

                        while(1)
                            if mf_mesos_setting.lock.acquire():
                                mf_mesos_setting.tasks_info_dict[task_id].action\
                                        = "aborting"
                                mf_mesos_setting.lock.release()
                                break

                        abort_executor_id = \
                                mf_mesos_setting.tasks_info_dict[task_id].\
                                executor_info.executor_id

                        abort_slave_id = \
                                mf_mesos_setting.tasks_info_dict[task_id].\
                                executor_info.slave_id

                        self.driver.sendFrameworkMessage(executor_id, slave_id,\
                                "[SCH_REQUEST] abort")

            else:
                time.sleep(1)


