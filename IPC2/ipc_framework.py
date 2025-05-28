"""
IPC Framework for 2-Process Architecture
Demonstrates various IPC mechanisms with performance testing
"""

import multiprocessing as mp
import threading
import time
import pickle
import struct
import socket
import mmap
import os
import numpy as np
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import queue


@dataclass
class TestData:
    """Sample data structure for testing"""
    config: Dict[str, Any]
    results: List[float]
    numpy_array: np.ndarray
    timestamp: float


class IPCBase(ABC):
    """Abstract base class for IPC mechanisms"""
    
    def __init__(self, name: str):
        self.name = name
        self.setup_time = 0
        self.send_times = []
        self.receive_times = []
    
    @abstractmethod
    def setup(self):
        """Setup the IPC mechanism"""
        pass
    
    @abstractmethod
    def send(self, data: Any):
        """Send data"""
        pass
    
    @abstractmethod
    def receive(self) -> Any:
        """Receive data"""
        pass
    
    @abstractmethod
    def cleanup(self):
        """Cleanup resources"""
        pass


class QueueIPC(IPCBase):
    """Queue-based IPC - your current working solution"""
    
    def __init__(self):
        super().__init__("Queue")
        self.to_worker = None
        self.from_worker = None
    
    def setup(self):
        start = time.time()
        self.to_worker = mp.Queue()
        self.from_worker = mp.Queue()
        self.setup_time = time.time() - start
    
    def send(self, data: Any):
        """Send from main to worker"""
        start = time.time()
        self.to_worker.put(data)
        self.send_times.append(time.time() - start)
    
    def receive(self) -> Any:
        """Receive in worker from main"""
        start = time.time()
        data = self.to_worker.get()
        self.receive_times.append(time.time() - start)
        return data
    
    def send_result(self, data: Any):
        """Send result from worker to main"""
        self.from_worker.put(data)
    
    def get_result(self) -> Any:
        """Get result in main from worker"""
        return self.from_worker.get()
    
    def cleanup(self):
        # Queues are automatically cleaned up
        pass


class PipeIPC(IPCBase):
    """Pipe-based IPC - bidirectional communication"""
    
    def __init__(self):
        super().__init__("Pipe")
        self.main_conn = None
        self.worker_conn = None
    
    def setup(self):
        start = time.time()
        # duplex=True creates bidirectional pipe
        self.main_conn, self.worker_conn = mp.Pipe(duplex=True)
        self.setup_time = time.time() - start
    
    def send(self, data: Any):
        """Send from main process"""
        start = time.time()
        self.main_conn.send(data)
        self.send_times.append(time.time() - start)
    
    def receive(self) -> Any:
        """Receive in worker process"""
        start = time.time()
        data = self.worker_conn.recv()
        self.receive_times.append(time.time() - start)
        return data
    
    def send_result(self, data: Any):
        """Send result from worker"""
        self.worker_conn.send(data)
    
    def get_result(self) -> Any:
        """Get result in main"""
        return self.main_conn.recv()
    
    def cleanup(self):
        if self.main_conn:
            self.main_conn.close()
        if self.worker_conn:
            self.worker_conn.close()


class SharedMemoryIPC(IPCBase):
    """Shared Memory IPC - highest performance for large data"""
    
    def __init__(self):
        super().__init__("SharedMemory")
        self.shm_blocks = {}
        self.control_queue = None  # For coordination
        self.result_queue = None
    
    def setup(self):
        start = time.time()
        # Use small queues for control messages only
        self.control_queue = mp.Queue()
        self.result_queue = mp.Queue()
        self.setup_time = time.time() - start
    
    def _create_shared_block(self, name: str, data: Any) -> str:
        """Create shared memory block for data"""
        if isinstance(data, np.ndarray):
            # For numpy arrays - most efficient
            shm = mp.shared_memory.SharedMemory(
                create=True, 
                size=data.nbytes,
                name=f"{name}_{os.getpid()}"
            )
            shared_array = np.ndarray(data.shape, dtype=data.dtype, buffer=shm.buf)
            shared_array[:] = data[:]
            self.shm_blocks[name] = shm
            return shm.name, data.shape, str(data.dtype)
        else:
            # For other data types, pickle and store
            pickled = pickle.dumps(data)
            shm = mp.shared_memory.SharedMemory(
                create=True,
                size=len(pickled),
                name=f"{name}_{os.getpid()}"
            )
            shm.buf[:len(pickled)] = pickled
            self.shm_blocks[name] = shm
            return shm.name, len(pickled), 'pickled'
    
    def send(self, data: TestData):
        """Send data via shared memory"""
        start = time.time()
        
        # Create shared memory blocks for each component
        shared_refs = {}
        
        # Handle numpy array efficiently
        if data.numpy_array is not None:
            shared_refs['numpy_array'] = self._create_shared_block('numpy_array', data.numpy_array)
        
        # Handle other data (config, results) via pickle
        other_data = {
            'config': data.config,
            'results': data.results,
            'timestamp': data.timestamp
        }
        shared_refs['other_data'] = self._create_shared_block('other_data', other_data)
        
        # Send references via control queue
        self.control_queue.put(shared_refs)
        self.send_times.append(time.time() - start)
    
    def receive(self) -> TestData:
        """Receive data from shared memory"""
        start = time.time()
        
        # Get shared memory references
        shared_refs = self.control_queue.get()
        
        # Reconstruct data
        numpy_array = None
        if 'numpy_array' in shared_refs:
            shm_name, shape, dtype = shared_refs['numpy_array']
            existing_shm = mp.shared_memory.SharedMemory(name=shm_name)
            numpy_array = np.ndarray(shape, dtype=np.dtype(dtype), buffer=existing_shm.buf).copy()
            existing_shm.close()
        
        # Get other data
        shm_name, size, data_type = shared_refs['other_data']
        existing_shm = mp.shared_memory.SharedMemory(name=shm_name)
        other_data = pickle.loads(bytes(existing_shm.buf[:size]))
        existing_shm.close()
        
        result = TestData(
            config=other_data['config'],
            results=other_data['results'],
            numpy_array=numpy_array,
            timestamp=other_data['timestamp']
        )
        
        self.receive_times.append(time.time() - start)
        return result
    
    def send_result(self, data: Any):
        """Send result back"""
        self.result_queue.put(data)
    
    def get_result(self) -> Any:
        """Get result"""
        return self.result_queue.get()
    
    def cleanup(self):
        """Cleanup shared memory blocks"""
        for shm in self.shm_blocks.values():
            try:
                shm.close()
                shm.unlink()
            except FileNotFoundError:
                pass  # Already cleaned up


class SocketIPC(IPCBase):
    """Socket-based IPC - most flexible, works across network"""
    
    def __init__(self):
        super().__init__("Socket")
        self.server_socket = None
        self.client_socket = None
        self.connection = None
        self.address = ('localhost', 0)  # Let OS choose port
    
    def setup(self):
        start = time.time()
        # Create server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(self.address)
        self.address = self.server_socket.getsockname()  # Get actual port
        self.server_socket.listen(1)
        self.setup_time = time.time() - start
    
    def _send_data(self, sock: socket.socket, data: Any):
        """Helper to send pickled data with length prefix"""
        pickled = pickle.dumps(data)
        # Send length first (4 bytes), then data
        sock.sendall(struct.pack('!I', len(pickled)))
        sock.sendall(pickled)
    
    def _recv_data(self, sock: socket.socket) -> Any:
        """Helper to receive pickled data with length prefix"""
        # Receive length first
        length_data = b''
        while len(length_data) < 4:
            chunk = sock.recv(4 - len(length_data))
            if not chunk:
                raise ConnectionError("Connection closed")
            length_data += chunk
        
        length = struct.unpack('!I', length_data)[0]
        
        # Receive actual data
        data = b''
        while len(data) < length:
            chunk = sock.recv(min(length - len(data), 4096))
            if not chunk:
                raise ConnectionError("Connection closed")
            data += chunk
        
        return pickle.loads(data)
    
    def connect_worker(self):
        """Call this in worker process to connect"""
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect(self.address)
    
    def accept_connection(self):
        """Call this in main process to accept worker connection"""
        self.connection, addr = self.server_socket.accept()
    
    def send(self, data: Any):
        """Send from main to worker"""
        start = time.time()
        self._send_data(self.connection, data)
        self.send_times.append(time.time() - start)
    
    def receive(self) -> Any:
        """Receive in worker from main"""
        start = time.time()
        data = self._recv_data(self.client_socket)
        self.receive_times.append(time.time() - start)
        return data
    
    def send_result(self, data: Any):
        """Send result from worker to main"""
        self._send_data(self.client_socket, data)
    
    def get_result(self) -> Any:
        """Get result in main from worker"""
        return self._recv_data(self.connection)
    
    def cleanup(self):
        """Cleanup sockets"""
        if self.connection:
            self.connection.close()
        if self.client_socket:
            self.client_socket.close()
        if self.server_socket:
            self.server_socket.close()


class PerformanceTester:
    """Test harness for IPC performance comparison"""
    
    def __init__(self):
        self.results = {}
    
    def create_test_data(self, size_mb: float = 1.0) -> TestData:
        """Create test data of specified size"""
        array_size = int((size_mb * 1024 * 1024) / 8)  # 8 bytes per float64
        return TestData(
            config={'param1': 'value1', 'param2': 42, 'nested': {'a': 1, 'b': 2}},
            results=[i * 0.1 for i in range(100)],
            numpy_array=np.random.rand(array_size).astype(np.float64),
            timestamp=time.time()
        )
    
    def worker_process(self, ipc: IPCBase, num_iterations: int, data_size_mb: float):
        """Worker process function"""
        try:
            if isinstance(ipc, SocketIPC):
                ipc.connect_worker()
            
            for i in range(num_iterations):
                # Receive data
                data = ipc.receive()
                
                # Simulate some work
                result = {
                    'iteration': i,
                    'processed_at': time.time(),
                    'array_sum': float(np.sum(data.numpy_array)) if data.numpy_array is not None else 0
                }
                
                # Send result back
                ipc.send_result(result)
                
        except Exception as e:
            print(f"Worker error: {e}")
        finally:
            if hasattr(ipc, 'cleanup'):
                ipc.cleanup()
    
    def test_ipc_mechanism(self, ipc_class, num_iterations: int = 10, data_size_mb: float = 1.0):
        """Test a specific IPC mechanism"""
        print(f"\nTesting {ipc_class.__name__}...")
        
        ipc = ipc_class()
        ipc.setup()
        
        start_time = time.time()
        
        try:
            # Start worker process
            if isinstance(ipc, SocketIPC):
                # For sockets, we need to handle connection setup
                worker = mp.Process(
                    target=self.worker_process,
                    args=(ipc, num_iterations, data_size_mb)
                )
                worker.start()
                ipc.accept_connection()
            else:
                worker = mp.Process(
                    target=self.worker_process,
                    args=(ipc, num_iterations, data_size_mb)
                )
                worker.start()
            
            # Main process sends data and collects results
            total_send_time = 0
            total_receive_time = 0
            
            for i in range(num_iterations):
                test_data = self.create_test_data(data_size_mb)
                
                # Send data
                send_start = time.time()
                ipc.send(test_data)
                total_send_time += time.time() - send_start
                
                # Get result
                recv_start = time.time()
                result = ipc.get_result()
                total_receive_time += time.time() - recv_start
            
            worker.join()
            total_time = time.time() - start_time
            
            # Calculate statistics
            avg_send_time = total_send_time / num_iterations * 1000  # ms
            avg_recv_time = total_receive_time / num_iterations * 1000  # ms
            throughput = (data_size_mb * num_iterations) / total_time  # MB/s
            
            results = {
                'setup_time_ms': ipc.setup_time * 1000,
                'avg_send_time_ms': avg_send_time,
                'avg_recv_time_ms': avg_recv_time,
                'total_time_s': total_time,
                'throughput_mb_s': throughput,
                'iterations': num_iterations,
                'data_size_mb': data_size_mb
            }
            
            self.results[ipc.name] = results
            
            print(f"  Setup time: {results['setup_time_ms']:.2f} ms")
            print(f"  Avg send time: {results['avg_send_time_ms']:.2f} ms")
            print(f"  Avg recv time: {results['avg_recv_time_ms']:.2f} ms")
            print(f"  Throughput: {results['throughput_mb_s']:.2f} MB/s")
            
        except Exception as e:
            print(f"Error testing {ipc.name}: {e}")
        finally:
            ipc.cleanup()
    
    def run_comprehensive_test(self):
        """Run tests on all IPC mechanisms"""
        ipc_classes = [QueueIPC, PipeIPC, SharedMemoryIPC, SocketIPC]
        
        print("IPC Performance Comparison")
        print("=" * 50)
        
        for ipc_class in ipc_classes:
            self.test_ipc_mechanism(ipc_class, num_iterations=5, data_size_mb=0.5)
        
        print("\n" + "=" * 50)
        print("SUMMARY")
        print("=" * 50)
        
        # Sort by throughput
        sorted_results = sorted(
            self.results.items(),
            key=lambda x: x[1]['throughput_mb_s'],
            reverse=True
        )
        
        for name, results in sorted_results:
            print(f"{name:15} | {results['throughput_mb_s']:8.2f} MB/s | "
                  f"Setup: {results['setup_time_ms']:6.2f}ms | "
                  f"Send: {results['avg_send_time_ms']:6.2f}ms")


if __name__ == "__main__":
    # Ensure multiprocessing works on all platforms
    mp.set_start_method('spawn', force=True)
    
    tester = PerformanceTester()
    tester.run_comprehensive_test()