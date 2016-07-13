import threading
import Queue

# variables need to be synchronized between threads
tasks_info_dict
executors_info_dict
lock
offers_queue

def init():
    tasks_info_dict = {}
    executors_info_dict = {}
    lock = threading.Lock()
    offers_queue = Queue.Queue()

# Makeflow Mesos task info class
class MfMesosTaskInfo:

    def __init__(self, task_id, cmd, inp_fns, oup_fns, action):
        self.task_id = task_id
        self.cmd = cmd
        self.inp_fns = inp_fns
        self.oup_fns = oup_fns
        self.action = action

# Makeflow Mesos executor info class
class MfMesosExecutorInfo:

    def __init__(self, executor_id, slave_id, hostname):
        self.executor_id = executor_id
        self.slave_id = slave_id
        self.hostname = hostname
