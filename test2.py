from multiprocessing.dummy import Pool as ThreadPool
from queue import Queue

def _master_worker(pool_in1:str):
    global q
    def _slave_worker(pool_in2:str):
        global q
        tmp = q.get_nowait()
        q.put(tmp)
        print(f'slave {tmp}')
        
        return int(pool_in2)
    
    k = list(pool_in1)
    
    tmp = q.get_nowait()
    q.put(tmp)
    print(f'master {tmp}')
    with ThreadPool(3) as pool2:
        return_worker = pool2.map(_slave_worker, k)
    return return_worker

def main(input_list):
    global q
    q = Queue()
    for i in range(11):
        q.put(i)
    with ThreadPool(3) as pool1:
        return_worker = pool1.map(_master_worker, input_list)
    print(return_worker)
    
if __name__ == '__main__':
    test=['123', '456', '789', '123', '456', '789', '123', '456', '789']
    
    main(test)