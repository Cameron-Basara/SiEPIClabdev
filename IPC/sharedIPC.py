from multiprocessing import Queue, Pipe, shared_memory, Manager
import socket
import os

def create_IPC(mode : str = "pipe"):
    """
    Stress test ICPs
    Returns a tuple suitable for two way communications
    Args:
        mode(str): pipe, queue, socket, shr_mem, manager
    """
    # For hb
    heartbeat_q = Queue() # "" on the Queue
    
    if mode == "pipe": 
        main_c, query_c = Pipe() # communication accross process
        return main_c, query_c, heartbeat_q
    
    elif mode == "queue":
        q1 = Queue()
        q2 = Queue()
        return q1, q2, heartbeat_q

    elif mode == "socket":
        parent_sock, child_sock = socket.socketpair()
        return parent_sock, child_sock, heartbeat_q

    elif mode == "shr_mem":
        size = 1024 * 1024 # 1 MB buffer
        # 1MB arr int32 elements shape of (262144,) (1D), (512, 512) (2D), or (16, 128, 128) (3D).
        # 1MB arr float64 elements shape of (131072,) (1D), (362, 362) (2D), or (16, 128, 64) (3D).
        shm = shared_memory.SharedMemory(create=True, size=size) # Os tracks shared mem buf
        return shm, shm, heartbeat_q 
    
    elif mode == "manager":
        manager = Manager()
        queue = manager.Queue()
        return queue, queue, heartbeat_q
    
    else:
        raise Exception("Mode type is not accepted, please select and available type")




