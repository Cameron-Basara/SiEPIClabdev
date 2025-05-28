"""
Integrated Multi-IPC System Architecture
Demonstrates using different IPC mechanisms for different purposes in a 2-process system
"""

import multiprocessing as mp
import threading
import time
import signal
import os
import numpy as np
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable
from enum import Enum
import json
import logging
from contextlib import contextmanager
import queue
import traceback


class TaskType(Enum):
    """Types of tasks the worker can handle"""
    COMPUTE = "compute"
    ANALYZE = "analyze" 
    TRANSFORM = "transform"
    SHUTDOWN = "shutdown"


class SystemState(Enum):
    """System states for coordination"""
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    SHUTTING_DOWN = "shutting_down"
    ERROR = "error"


@dataclass
class Task:
    """Task definition for worker process"""
    task_id: str
    task_type: TaskType
    parameters: Dict[str, Any]
    data: Optional[np.ndarray] = None
    priority: int = 0
    created_at: float = field(default_factory=time.time)


@dataclass
class Result:
    """Result from worker process"""
    task_id: str
    success: bool
    data: Optional[np.ndarray] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    processing_time: float = 0.0
    completed_at: float = field(default_factory=time.time)
    error_message: Optional[str] = None


@dataclass
class SystemConfig:
    """System configuration"""
    max_workers: int = 1
    batch_size: int = 10
    timeout: float = 30.0
    debug_mode: bool = False
    processing_params: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0.0"


class IPCCoordinator:
    """Coordinates multiple IPC mechanisms for different purposes"""
    
    def __init__(self, config: SystemConfig):
        self.config = config
        self.worker_pid = None
        
        # Different IPC mechanisms for different purposes
        self._setup_control_channel()      # Signals for basic control
        self._setup_config_sharing()       # Shared memory for configuration
        self._setup_task_queue()          # Queue for task distribution
        self._setup_result_collection()   # Pipe for result collection
        self._setup_large_data_transfer() # Shared memory for large data
        self._setup_watchdog_system()     # Memory-mapped file for health monitoring
        
        # Coordination
        self.shutdown_event = mp.Event()
        self.system_state = mp.Value('i', SystemState.INITIALIZING.value)
        
    def _setup_control_channel(self):
        """Setup signal-based control for immediate commands"""
        self.control_signals = {
            'pause': signal.SIGUSR1,
            'resume': signal.SIGUSR2,
            'reload_config': signal.SIGTERM  # We'll catch and handle gracefully
        }
        
    def _setup_config_sharing(self):
        """Setup shared memory for configuration that both processes can access"""
        self.config_manager = mp.Manager()
        self.shared_config = self.config_manager.dict()
        self.config_lock = self.config_manager.Lock()
        
        # Initialize shared config
        with self.config_lock:
            self.shared_config.update({
                'max_workers': self.config.max_workers,
                'batch_size': self.config.batch_size,
                'timeout': self.config.timeout,
                'debug_mode': self.config.debug_mode,
                'processing_params': self.config.processing_params,
                'version': self.config.version,
                'last_updated': time.time()
            })
    
    def _setup_task_queue(self):
        """Setup high-reliability queue for task distribution"""
        self.task_queue = mp.Queue(maxsize=100)  # Bounded queue to prevent memory issues
        
    def _setup_result_collection(self):
        """Setup pipe for efficient result collection"""
        self.result_sender, self.result_receiver = mp.Pipe(duplex=False)
        
    def _setup_large_data_transfer(self):
        """Setup shared memory for large numpy arrays"""
        self.shared_arrays = {}  # Track shared memory blocks
        self.array_metadata_queue = mp.Queue()  # For sharing array metadata
        
    def _setup_watchdog_system(self):
        """Setup memory-mapped file for health monitoring"""
        import tempfile
        import mmap
        
        # Create temporary file for watchdog
        self.watchdog_file = tempfile.NamedTemporaryFile(delete=False)
        self.watchdog_file.write(b'\x00' * 1024)  # 1KB for watchdog data
        self.watchdog_file.flush()
        
        self.watchdog_mmap = mmap.mmap(
            self.watchdog_file.fileno(),
            1024,
            access=mmap.ACCESS_WRITE
        )
        
    def update_config(self, updates: Dict[str, Any]):
        """Update shared configuration"""
        with self.config_lock:
            self.shared_config.update(updates)
            self.shared_config['last_updated'] = time.time()
            
        # Signal worker to reload config if it's running
        if self.worker_pid:
            try:
                os.kill(self.worker_pid, self.control_signals['reload_config'])
            except ProcessLookupError:
                pass  # Worker process has died
                
    def get_config(self) -> Dict[str, Any]:
        """Get current configuration"""
        with self.config_lock:
            return dict(self.shared_config)
            
    def send_task(self, task: Task):
        """Send task to worker via queue"""
        try:
            # Handle large data separately via shared memory
            if task.data is not None and task.data.size > 10000:  # > 10K elements
                shared_name = self._store_array_in_shared_memory(task.data, task.task_id)
                # Remove data from task and add reference
                task_copy = Task(
                    task_id=task.task_id,
                    task_type=task.task_type,
                    parameters={**task.parameters, '_shared_array_key': shared_name},
                    data=None,  # Remove large data
                    priority=task.priority,
                    created_at=task.created_at
                )
                self.task_queue.put(task_copy, timeout=5.0)
            else:
                self.task_queue.put(task, timeout=5.0)
                
        except queue.Full:
            raise RuntimeError("Task queue is full - worker may be stuck")
            
    def _store_array_in_shared_memory(self, array: np.ndarray, key: str) -> str:
        """Store numpy array in shared memory"""
        try:
            shm = mp.shared_memory.SharedMemory(
                create=True,
                size=array.nbytes,
                name=f"array_{key}_{os.getpid()}"
            )
            
            # Copy array to shared memory
            shared_array = np.ndarray(array.shape, dtype=array.dtype, buffer=shm.buf)
            shared_array[:] = array[:]
            
            # Store reference
            self.shared_arrays[key] = shm
            
            # Send metadata to worker
            metadata = {
                'key': key,
                'shared_name': shm.name,
                'shape': array.shape,
                'dtype': str(array.dtype)
            }
            self.array_metadata_queue.put(metadata)
            
            return shm.name
            
        except Exception as e:
            logging.error(f"Failed to store array in shared memory: {e}")
            raise
            
    def get_result(self, timeout: float = None) -> Optional[Result]:
        """Get result from worker via pipe"""
        try:
            if self.result_receiver.poll(timeout):
                return self.result_receiver.recv()
        except EOFError:
            return None  # Pipe closed
        return None
        
    def update_watchdog(self, data: Dict[str, Any]):
        """Update watchdog information"""
        try:
            watchdog_info = {
                'timestamp': time.time(),
                'pid': os.getpid(),
                'state': data.get('state', 'unknown'),
                'current_task': data.get('current_task', None),
                'memory_usage': data.get('memory_usage', 0),
                'tasks_processed': data.get('tasks_processed', 0)
            }
            
            json_data = json.dumps(watchdog_info).encode('utf-8')
            self.watchdog_mmap.seek(0)
            self.watchdog_mmap.write(json_data.ljust(1024, b'\x00'))
            self.watchdog_mmap.flush()
            
        except Exception as e:
            logging.error(f"Failed to update watchdog: {e}")
            
    def read_watchdog(self) -> Optional[Dict[str, Any]]:
        """Read watchdog information"""
        try:
            self.watchdog_mmap.seek(0)
            data = self.watchdog_mmap.read(1024).rstrip(b'\x00')
            if data:
                return json.loads(data.decode('utf-8'))
        except Exception as e:
            logging.error(f"Failed to read watchdog: {e}")
        return None
        
    def send_control_signal(self, signal_type: str):
        """Send control signal to worker"""
        if self.worker_pid and signal_type in self.control_signals:
            try:
                os.kill(self.worker_pid, self.control_signals[signal_type])
            except ProcessLookupError:
                logging.warning("Worker process not found for signal")
                
    def cleanup(self):
        """Cleanup all IPC resources"""
        # Close pipes
        if hasattr(self, 'result_sender'):
            self.result_sender.close()
        if hasattr(self, 'result_receiver'):
            self.result_receiver.close()
            
        # Cleanup shared memory
        for shm in self.shared_arrays.values():
            try:
                shm.close()
                shm.unlink()
            except FileNotFoundError:
                pass
                
        # Cleanup watchdog
        if hasattr(self, 'watchdog_mmap'):
            self.watchdog_mmap.close()
        if hasattr(self, 'watchdog_file'):
            self.watchdog_file.close()
            os.unlink(self.watchdog_file.name)
            
        # Cleanup manager
        if hasattr(self, 'config_manager'):
            self.config_manager.shutdown()


class MainProcess:
    """Main process that coordinates the system"""
    
    def __init__(self, initial_config: SystemConfig):
        self.config = initial_config
        self.ipc = IPCCoordinator(initial_config)
        self.worker_process = None
        self.task_counter = 0
        self.results = []
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('MainProcess')
        
    def start_worker(self):
        """Start the worker process"""
        self.worker_process = mp.Process(
            target=worker_main,
            args=(self.ipc,),
            name='WorkerProcess'
        )
        self.worker_process.start()
        self.ipc.worker_pid = self.worker_process.pid
        self.logger.info(f"Started worker process with PID: {self.worker_process.pid}")
        
    def submit_task(self, task_type: TaskType, parameters: Dict[str, Any], 
                   data: Optional[np.ndarray] = None, priority: int = 0) -> str:
        """Submit a task to the worker"""
        task_id = f"task_{self.task_counter:06d}"
        self.task_counter += 1
        
        task = Task(
            task_id=task_id,
            task_type=task_type,
            parameters=parameters,
            data=data,
            priority=priority
        )
        
        self.ipc.send_task(task)
        self.logger.info(f"Submitted task {task_id} of type {task_type.value}")
        return task_id
        
    def collect_results(self, timeout: float = 1.0) -> List[Result]:
        """Collect available results"""
        new_results = []
        while True:
            result = self.ipc.get_result(timeout=timeout)
            if result is None:
                break
            new_results.append(result)
            self.results.append(result)
            self.logger.info(f"Received result for task {result.task_id}")
            
        return new_results
        
    def update_configuration(self, updates: Dict[str, Any]):
        """Update system configuration"""
        self.logger.info(f"Updating configuration: {updates}")
        self.ipc.update_config(updates)
        
    def pause_worker(self):
        """Pause worker processing"""
        self.logger.info("Pausing worker")
        self.ipc.send_control_signal('pause')
        
    def resume_worker(self):
        """Resume worker processing"""
        self.logger.info("Resuming worker")
        self.ipc.send_control_signal('resume')
        
    def get_worker_status(self) -> Optional[Dict[str, Any]]:
        """Get current worker status from watchdog"""
        watchdog_data = self.ipc.read_watchdog()
        if watchdog_data:
            # Check if worker is alive based on timestamp
            time_since_update = time.time() - watchdog_data.get('timestamp', 0)
            watchdog_data['is_alive'] = time_since_update < 10.0  # 10 second timeout
            
        return watchdog_data
        
    def shutdown(self):
        """Gracefully shutdown the system"""
        self.logger.info("Initiating system shutdown")
        
        # Send shutdown task
        self.submit_task(TaskType.SHUTDOWN, {})
        
        # Wait for worker to finish
        if self.worker_process:
            self.worker_process.join(timeout=10.0)
            if self.worker_process.is_alive():
                self.logger.warning("Worker didn't shutdown gracefully, terminating")
                self.worker_process.terminate()
                self.worker_process.join()
                
        # Cleanup IPC resources
        self.ipc.cleanup()
        self.logger.info("System shutdown complete")
        
    def run_monitoring_loop(self, duration: float = 60.0):
        """Run main monitoring loop"""
        start_time = time.time()
        
        while time.time() - start_time < duration:
            # Collect results
            results = self.collect_results(timeout=0.1)
            
            # Check worker status
            status = self.get_worker_status()
            if status:
                if not status.get('is_alive', False):
                    self.logger.error("Worker appears to be dead!")
                    break
                else:
                    self.logger.debug(f"Worker status: {status['state']}, "
                                    f"tasks processed: {status.get('tasks_processed', 0)}")
            
            time.sleep(1.0)


def setup_worker_signals(ipc_coordinator):
    """Setup signal handlers in worker process"""
    paused = threading.Event()
    
    def handle_pause(signum, frame):
        logging.info("Worker paused by signal")
        paused.set()
        
    def handle_resume(signum, frame):
        logging.info("Worker resumed by signal")
        paused.clear()
        
    def handle_reload_config(signum, frame):
        logging.info("Worker reloading configuration")
        # Config is automatically updated via shared memory
        
    signal.signal(signal.SIGUSR1, handle_pause)
    signal.signal(signal.SIGUSR2, handle_resume)
    signal.signal(signal.SIGTERM, handle_reload_config)
    
    return paused


def worker_main(ipc_coordinator: IPCCoordinator):
    """Main function for worker process"""
    # Setup logging for worker
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - WORKER - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger('WorkerProcess')
    
    # Setup signal handlers
    paused_event = setup_worker_signals(ipc_coordinator)
    
    # Worker state
    tasks_processed = 0
    current_task = None
    shared_arrays = {}  # Local cache of shared arrays
    
    logger.info("Worker process started")
    
    try:
        while True:
            # Update watchdog
            ipc_coordinator.update_watchdog({
                'state': 'paused' if paused_event.is_set() else 'running',
                'current_task': current_task,
                'memory_usage': 0,  # Could add actual memory monitoring
                'tasks_processed': tasks_processed
            })
            
            # Wait if paused
            while paused_event.is_set():
                time.sleep(0.1)
                continue
                
            try:
                # Get task with timeout
                task = ipc_coordinator.task_queue.get(timeout=1.0)
                current_task = task.task_id
                
                logger.info(f"Processing task {task.task_id} of type {task.task_type.value}")
                
                # Handle shutdown
                if task.task_type == TaskType.SHUTDOWN:
                    logger.info("Received shutdown signal")
                    break
                    
                # Get large data if needed
                task_data = task.data
                if '_shared_array_key' in task.parameters:
                    array_key = task.parameters['_shared_array_key']
                    if array_key not in shared_arrays:
                        # Get array metadata
                        metadata = ipc_coordinator.array_metadata_queue.get(timeout=5.0)
                        if metadata['key'] == task.parameters.get('_shared_array_key').split('_')[-2]:
                            # Connect to shared memory
                            existing_shm = mp.shared_memory.SharedMemory(name=metadata['shared_name'])
                            task_data = np.ndarray(
                                metadata['shape'], 
                                dtype=np.dtype(metadata['dtype']), 
                                buffer=existing_shm.buf
                            ).copy()  # Make a copy to avoid issues
                            shared_arrays[array_key] = existing_shm
                
                # Process task
                start_time = time.time()
                result = process_task(task, task_data, ipc_coordinator.get_config())
                processing_time = time.time() - start_time
                
                # Send result
                result.processing_time = processing_time
                ipc_coordinator.result_sender.send(result)
                
                tasks_processed += 1
                current_task = None
                
                logger.info(f"Completed task {task.task_id} in {processing_time:.3f}s")
                
            except queue.Empty:
                continue  # No task available, continue monitoring
            except Exception as e:
                logger.error(f"Error processing task: {e}")
                logger.error(traceback.format_exc())
                
                # Send error result
                if current_task:
                    error_result = Result(
                        task_id=current_task,
                        success=False,
                        error_message=str(e)
                    )
                    ipc_coordinator.result_sender.send(error_result)
                    
    except Exception as e:
        logger.error(f"Worker crashed: {e}")
        logger.error(traceback.format_exc())
    finally:
        # Cleanup shared arrays
        for shm in shared_arrays.values():
            try:
                shm.close()
            except:
                pass
                
        logger.info("Worker process exiting")


def process_task(task: Task, data: Optional[np.ndarray], config: Dict[str, Any]) -> Result:
    """Process a task based on its type"""
    
    try:
        if task.task_type == TaskType.COMPUTE:
            # Example computation task
            if data is not None:
                result_data = np.mean(data, axis=0) if data.ndim > 1 else np.array([np.mean(data)])
            else:
                result_data = np.array([42.0])  # Default result
                
            return Result(
                task_id=task.task_id,
                success=True,
                data=result_data,
                metadata={'operation': 'mean', 'input_shape': data.shape if data is not None else None}
            )
            
        elif task.task_type == TaskType.ANALYZE:
            # Example analysis task
            if data is not None:
                stats = {
                    'mean': float(np.mean(data)),
                    'std': float(np.std(data)),
                    'min': float(np.min(data)),
                    'max': float(np.max(data))
                }
            else:
                stats = {'error': 'No data provided'}
                
            return Result(
                task_id=task.task_id,
                success=True,
                metadata=stats
            )
            
        elif task.task_type == TaskType.TRANSFORM:
            # Example transformation task
            if data is not None:
                # Apply some transformation
                transformed = data * task.parameters.get('scale_factor', 1.0)
                transformed += task.parameters.get('offset', 0.0)
                result_data = transformed
            else:
                result_data = None
                
            return Result(
                task_id=task.task_id,
                success=True,
                data=result_data,
                metadata={'transform': 'scale_and_offset'}
            )
            
        else:
            return Result(
                task_id=task.task_id,
                success=False,
                error_message=f"Unknown task type: {task.task_type}"
            )
            
    except Exception as e:
        return Result(
            task_id=task.task_id,
            success=False,
            error_message=str(e)
        )


# Example usage and demonstration
if __name__ == "__main__":
    # Ensure proper multiprocessing
    mp.set_start_method('spawn', force=True)
    
    # Create initial configuration
    config = SystemConfig(
        max_workers=1,
        batch_size=5,
        timeout=30.0,
        debug_mode=True,
        processing_params={'default_scale': 1.0}
    )
    
    # Create and start main process
    main = MainProcess(config)
    
    try:
        main.start_worker()
        
        # Submit various types of tasks
        print("Submitting tasks...")
        
        # Small data task (will use queue)
        main.submit_task(
            TaskType.COMPUTE,
            {'operation': 'mean'},
            data=np.random.rand(100)
        )
        
        # Large data task (will use shared memory)
        main.submit_task(
            TaskType.ANALYZE,
            {'detailed': True},
            data=np.random.rand(50000)  # Large array
        )
        
        # Transform task
        main.submit_task(
            TaskType.TRANSFORM,
            {'scale_factor': 2.0, 'offset': 1.0},
            data=np.random.rand(1000, 10)
        )
        
        # Let tasks process
        print("Processing tasks...")
        time.sleep(2)
        
        # Collect results
        results = main.collect_results()
        print(f"Collected {len(results)} results")
        
        # Demonstrate configuration update
        print("Updating configuration...")
        main.update_configuration({'batch_size': 10, 'debug_mode': False})
        
        # Demonstrate pause/resume
        print("Testing pause/resume...")
        main.pause_worker()
        time.sleep(1)
        main.resume_worker()
        
        # Check worker status
        status = main.get_worker_status()
        if status:
            print(f"Worker status: {status}")
        
        # Submit more tasks to test updated config
        for i in range(3):
            main.submit_task(
                TaskType.COMPUTE,
                {'iteration': i},
                data=np.random.rand(200)
            )
            
        # Final collection
        time.sleep(2)
        final_results = main.collect_results()
        print(f"Final collection: {len(final_results)} new results")
        print(f"Total results processed: {len(main.results)}")
        
    finally:
        main.shutdown()