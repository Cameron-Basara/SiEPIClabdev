"""
Advanced IPC Options and Specialized Implementations
Demonstrates additional IPC mechanisms and optimization techniques
"""

import multiprocessing as mp
import threading
import time
import mmap
import os
import tempfile
import signal
from multiprocessing import shared_memory
from multiprocessing.managers import BaseManager
import zmq  
import redis 
from abc import ABC, abstractmethod
import numpy as np
from typing import Any, Dict, Optional
import json


class MemoryMappedFileIPC:
    """Memory-mapped file IPC - persistent shared memory"""
    
    def __init__(self, file_size: int = 1024 * 1024):  # 1MB default
        self.file_size = file_size
        self.temp_file = None
        self.mmap_obj = None
        self.lock = mp.Lock()
        
    def setup(self):
        """Create memory-mapped file"""
        # Create temporary file
        self.temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.temp_file.write(b'\x00' * self.file_size)
        self.temp_file.flush()
        
        # Create memory mapping
        self.mmap_obj = mmap.mmap(
            self.temp_file.fileno(), 
            self.file_size,
            access=mmap.ACCESS_WRITE
        )
    
    def write_data(self, data: bytes, offset: int = 0):
        """Write data to memory-mapped region"""
        with self.lock:
            self.mmap_obj.seek(offset)
            self.mmap_obj.write(data)
            self.mmap_obj.flush()
    
    def read_data(self, size: int, offset: int = 0) -> bytes:
        """Read data from memory-mapped region"""
        with self.lock:
            self.mmap_obj.seek(offset)
            return self.mmap_obj.read(size)
    
    def cleanup(self):
        """Cleanup resources"""
        if self.mmap_obj:
            self.mmap_obj.close()
        if self.temp_file:
            self.temp_file.close()
            os.unlink(self.temp_file.name)


class SignalBasedIPC:
    """Signal-based IPC - for simple notifications and coordination"""
    
    def __init__(self):
        self.signals_received = []
        self.setup_handlers()
    
    def setup_handlers(self):
        """Setup signal handlers"""
        signal.signal(signal.SIGUSR1, self._handle_signal)
        signal.signal(signal.SIGUSR2, self._handle_signal)
    
    def _handle_signal(self, signum, frame):
        """Handle received signals"""
        self.signals_received.append({
            'signal': signum,
            'timestamp': time.time(),
            'pid': os.getpid()
        })
    
    def send_signal(self, target_pid: int, signal_type: int = signal.SIGUSR1):
        """Send signal to target process"""
        os.kill(target_pid, signal_type)
    
    def wait_for_signal(self, timeout: float = 5.0) -> bool:
        """Wait for signal with timeout"""
        start_time = time.time()
        initial_count = len(self.signals_received)
        
        while time.time() - start_time < timeout:
            if len(self.signals_received) > initial_count:
                return True
            time.sleep(0.01)
        return False


class CustomManagerIPC:
    """Custom multiprocessing Manager - for complex shared objects"""
    
    def __init__(self):
        self.manager = None
        self.shared_dict = None
        self.shared_list = None
        self.shared_lock = None
    
    def setup(self):
        """Setup custom manager with shared objects"""
        self.manager = mp.Manager()
        self.shared_dict = self.manager.dict()
        self.shared_list = self.manager.list()
        self.shared_lock = self.manager.Lock()
        
        # Initialize shared state
        self.shared_dict['config'] = {}
        self.shared_dict['status'] = 'initialized'
        self.shared_dict['watchdog'] = time.time()
    
    def update_config(self, config: Dict[str, Any]):
        """Thread-safe config update"""
        with self.shared_lock:
            self.shared_dict['config'].update(config)
            self.shared_dict['watchdog'] = time.time()
    
    def get_config(self) -> Dict[str, Any]:
        """Get current config"""
        with self.shared_lock:
            return dict(self.shared_dict['config'])
    
    def add_result(self, result: Any):
        """Add result to shared list"""
        with self.shared_lock:
            self.shared_list.append(result)
    
    def get_results(self) -> list:
        """Get all results"""
        with self.shared_lock:
            return list(self.shared_list)
    
    def update_watchdog(self):
        """Update watchdog timestamp"""
        self.shared_dict['watchdog'] = time.time()
    
    def check_watchdog(self, timeout: float = 5.0) -> bool:
        """Check if watchdog is alive"""
        return time.time() - self.shared_dict['watchdog'] < timeout
    
    def cleanup(self):
        """Cleanup manager"""
        if self.manager:
            self.manager.shutdown()


class ZeroMQIPC:
    """ZeroMQ-based IPC - high-performance messaging library"""
    
    def __init__(self, pattern: str = 'push_pull'):
        self.pattern = pattern
        self.context = None
        self.socket = None
        self.address = "tcp://127.0.0.1:5555"
    
    def setup_push_pull(self, is_pusher: bool = True):
        """Setup PUSH/PULL pattern (one-way, load-balanced)"""
        self.context = zmq.Context()
        
        if is_pusher:
            self.socket = self.context.socket(zmq.PUSH)
            self.socket.bind(self.address)
        else:
            self.socket = self.context.socket(zmq.PULL)
            self.socket.connect(self.address)
    
    def setup_req_rep(self, is_server: bool = True):
        """Setup REQ/REP pattern (request-response)"""
        self.context = zmq.Context()
        
        if is_server:
            self.socket = self.context.socket(zmq.REP)
            self.socket.bind(self.address)
        else:
            self.socket = self.context.socket(zmq.REQ)
            self.socket.connect(self.address)
    
    def setup_pub_sub(self, is_publisher: bool = True, topic: str = ""):
        """Setup PUB/SUB pattern (broadcast)"""
        self.context = zmq.Context()
        
        if is_publisher:
            self.socket = self.context.socket(zmq.PUB)
            self.socket.bind(self.address)
        else:
            self.socket = self.context.socket(zmq.SUB)
            self.socket.connect(self.address)
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)
    
    def send_json(self, data: Any):
        """Send JSON data"""
        self.socket.send_json(data)
    
    def recv_json(self) -> Any:
        """Receive JSON data"""
        return self.socket.recv_json()
    
    def send_numpy(self, array: np.ndarray):
        """Send numpy array efficiently"""
        # Send metadata first
        metadata = {
            'dtype': str(array.dtype),
            'shape': array.shape
        }
        self.socket.send_json(metadata, zmq.SNDMORE)
        # Send array data
        self.socket.send(array.tobytes())
    
    def recv_numpy(self) -> np.ndarray:
        """Receive numpy array"""
        # Receive metadata
        metadata = self.socket.recv_json()
        # Receive array data
        data = self.socket.recv()
        # Reconstruct array
        return np.frombuffer(data, dtype=metadata['dtype']).reshape(metadata['shape'])
    
    def cleanup(self):
        """Cleanup ZeroMQ resources"""
        if self.socket:
            self.socket.close()
        if self.context:
            self.context.term()


class RedisIPC:
    """Redis-based IPC - external message broker"""
    
    def __init__(self, host: str = 'localhost', port: int = 6379):
        self.host = host
        self.port = port
        self.redis_conn = None
        self.pubsub = None
    
    def setup(self):
        """Setup Redis connection"""
        try:
            import redis
            self.redis_conn = redis.Redis(
                host=self.host, 
                port=self.port, 
                decode_responses=True
            )
            # Test connection
            self.redis_conn.ping()
        except (ImportError, redis.ConnectionError) as e:
            print(f"Redis not available: {e}")
            return False
        return True
    
    def send_message(self, channel: str, message: Any):
        """Send message to Redis channel"""
        if isinstance(message, dict):
            message = json.dumps(message)
        self.redis_conn.publish(channel, message)
    
    def subscribe_channel(self, channel: str):
        """Subscribe to Redis channel"""
        self.pubsub = self.redis_conn.pubsub()
        self.pubsub.subscribe(channel)
    
    def get_message(self, timeout: float = 1.0) -> Optional[Any]:
        """Get message from subscribed channel"""
        if not self.pubsub:
            return None
            
        message = self.pubsub.get_message(timeout=timeout)