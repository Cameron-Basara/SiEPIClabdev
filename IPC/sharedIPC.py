from multiprocessing import Queue, Pipe

def create_ICP():
    main_c, query_c = Pipe() # communication accross process
    heartbeat_q = Queue() # "" on the Queue
    return main_c, query_c, heartbeat_q



