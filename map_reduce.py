import logging
import threading
import queue
import time
import json
import os
import importlib.util
import inspect
from collections import defaultdict

logger = logging.getLogger(__name__)

class MapReduce:
    """
    Simple MapReduce implementation for distributed data processing.
    """
    
    def __init__(self, data_store, worker_count=4, is_leader=False, peer_stubs=None):
        """
        Initialize the MapReduce engine.
        
        Args:
            data_store: Reference to the distributed data store
            worker_count: Number of local worker threads
            is_leader: Whether this node is the leader for coordinating jobs
            peer_stubs: gRPC stubs for communicating with peer nodes
        """
        self.data_store = data_store
        self.worker_count = worker_count
        self.is_leader = is_leader
        self.peer_stubs = peer_stubs or {}
        
        # Task queues
        self.map_task_queue = queue.Queue()
        self.reduce_task_queue = queue.Queue()
        
        # Workers
        self.map_workers = []
        self.reduce_workers = []
        
        # Results
        self.map_results = {}
        self.reduce_results = {}
        
        # Job state
        self.active_job = None
        self.job_state = {}
        
        # Locks
        self.job_lock = threading.RLock()
        
        # Start workers
        self._start_workers()
        
        logger.info(f"MapReduce initialized with {worker_count} workers")
    
    def _start_workers(self):
        """Start worker threads for map and reduce tasks."""
        # Map workers
        for i in range(self.worker_count):
            worker = threading.Thread(
                target=self._map_worker_loop,
                name=f"MapWorker-{i}"
            )
            worker.daemon = True
            worker.start()
            self.map_workers.append(worker)
        
        # Reduce workers
        for i in range(self.worker_count):
            worker = threading.Thread(
                target=self._reduce_worker_loop,
                name=f"ReduceWorker-{i}"
            )
            worker.daemon = True
            worker.start()
            self.reduce_workers.append(worker)
    
    def _map_worker_loop(self):
        """Worker loop for processing map tasks."""
        while True:
            try:
                # Get a task from the queue
                task = self.map_task_queue.get()
                
                if task is None:
                    # Poison pill, exit the loop
                    break
                
                job_id, mapper_name, input_key, input_value = task
                
                try:
                    # Load the mapper
                    mapper = self._load_mapper(mapper_name)
                    
                    # Execute the map function
                    intermediate_results = mapper(input_key, input_value)
                    
                    # Group results by key for the reduce phase
                    with self.job_lock:
                        if job_id in self.job_state:
                            job_state = self.job_state[job_id]
                            
                            # Group by key
                            for k, v in intermediate_results:
                                job_state['intermediate'].setdefault(k, []).append(v)
                            
                            # Update completed map tasks count
                            job_state['completed_maps'] += 1
                            
                            # Check if all map tasks are done
                            if job_state['completed_maps'] >= job_state['total_maps']:
                                # Start reduce phase
                                self._schedule_reduce_tasks(job_id)
                    
                    logger.debug(f"Completed map task for job {job_id}: {input_key}")
                
                except Exception as e:
                    logger.error(f"Error in map task {job_id}/{input_key}: {e}")
                
                finally:
                    # Mark task as done
                    self.map_task_queue.task_done()
            
            except Exception as e:
                logger.error(f"Error in map worker: {e}")
    
    def _reduce_worker_loop(self):
        """Worker loop for processing reduce tasks."""
        while True:
            try:
                # Get a task from the queue
                task = self.reduce_task_queue.get()
                
                if task is None:
                    # Poison pill, exit the loop
                    break
                
                job_id, reducer_name, key, values = task
                
                try:
                    # Load the reducer
                    reducer = self._load_reducer(reducer_name)
                    
                    # Execute the reduce function
                    result = reducer(key, values)
                    
                    # Store the result
                    with self.job_lock:
                        if job_id in self.job_state:
                            job_state = self.job_state[job_id]
                            job_state['results'][key] = result
                            
                            # Update completed reduce tasks count
                            job_state['completed_reduces'] += 1
                            
                            # Check if all reduce tasks are done
                            if job_state['completed_reduces'] >= job_state['total_reduces']:
                                # Job complete
                                self._finish_job(job_id)
                    
                    logger.debug(f"Completed reduce task for job {job_id}: {key}")
                
                except Exception as e:
                    logger.error(f"Error in reduce task {job_id}/{key}: {e}")
                
                finally:
                    # Mark task as done
                    self.reduce_task_queue.task_done()
            
            except Exception as e:
                logger.error(f"Error in reduce worker: {e}")
    
    def _load_mapper(self, mapper_name):
        """
        Load a mapper function.
        
        Args:
            mapper_name: Name of the mapper (module.function)
            
        Returns:
            The mapper function
        """
        try:
            # Try loading from a predefined set first
            if hasattr(self, f"_mapper_{mapper_name}"):
                return getattr(self, f"_mapper_{mapper_name}")
            
            # Otherwise, load from a module
            module_name, func_name = mapper_name.rsplit('.', 1)
            module_path = f"analytics/mappers/{module_name}.py"
            
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            return getattr(module, func_name)
        
        except Exception as e:
            logger.error(f"Error loading mapper {mapper_name}: {e}")
            raise
    
    def _load_reducer(self, reducer_name):
        """
        Load a reducer function.
        
        Args:
            reducer_name: Name of the reducer (module.function)
            
        Returns:
            The reducer function
        """
        try:
            # Try loading from a predefined set first
            if hasattr(self, f"_reducer_{reducer_name}"):
                return getattr(self, f"_reducer_{reducer_name}")
            
            # Otherwise, load from a module
            module_name, func_name = reducer_name.rsplit('.', 1)
            module_path = f"analytics/reducers/{module_name}.py"
            
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            return getattr(module, func_name)
        
        except Exception as e:
            logger.error(f"Error loading reducer {reducer_name}: {e}")
            raise
    
    def _schedule_reduce_tasks(self, job_id):
        """
        Schedule reduce tasks for a job.
        
        Args:
            job_id: The job ID
        """
        with self.job_lock:
            if job_id not in self.job_state:
                return
            
            job_state = self.job_state[job_id]
            
            # Get intermediate results grouped by key
            intermediate = job_state['intermediate']
            
            # Count the total number of reduce tasks
            job_state['total_reduces'] = len(intermediate)
            
            # Schedule a reduce task for each key
            for key, values in intermediate.items():
                self.reduce_task_queue.put((
                    job_id,
                    job_state['reducer_name'],
                    key,
                    values
                ))
            
            logger.info(f"Scheduled {len(intermediate)} reduce tasks for job {job_id}")
    
    def _finish_job(self, job_id):
        """
        Finish a MapReduce job.
        
        Args:
            job_id: The job ID
        """
        with self.job_lock:
            if job_id not in self.job_state:
                return
            
            job_state = self.job_state[job_id]
            
            # Set job status to completed
            job_state['status'] = 'completed'
            job_state['end_time'] = time.time()
            
            # Store results
            self.reduce_results[job_id] = job_state['results']
            
            # Save results to data store
            self.data_store.put('mapreduce_results', job_id, job_state['results'])
            
            logger.info(f"Job {job_id} completed in {job_state['end_time'] - job_state['start_time']:.2f} seconds")
    
    def run_job(self, job_id, mapper_name, reducer_name, input_data):
        """
        Run a MapReduce job.
        
        Args:
            job_id: Unique job identifier
            mapper_name: Name of the mapper function
            reducer_name: Name of the reducer function
            input_data: Dictionary of input data {key: value}
            
        Returns:
            Job ID for tracking
        """
        with self.job_lock:
            # Initialize job state
            self.job_state[job_id] = {
                'status': 'running',
                'start_time': time.time(),
                'end_time': None,
                'mapper_name': mapper_name,
                'reducer_name': reducer_name,
                'total_maps': len(input_data),
                'completed_maps': 0,
                'total_reduces': 0,
                'completed_reduces': 0,
                'intermediate': defaultdict(list),
                'results': {}
            }
            
            # Schedule map tasks
            for key, value in input_data.items():
                self.map_task_queue.put((
                    job_id,
                    mapper_name,
                    key,
                    value
                ))
            
            logger.info(f"Started job {job_id} with {len(input_data)} map tasks")
            
            return job_id
    
    def get_job_status(self, job_id):
        """
        Get the status of a job.
        
        Args:
            job_id: The job ID
            
        Returns:
            Dictionary with job status information
        """
        with self.job_lock:
            if job_id not in self.job_state:
                return {'status': 'not_found'}
            
            job_state = self.job_state[job_id]
            
            return {
                'status': job_state['status'],
                'start_time': job_state['start_time'],
                'end_time': job_state['end_time'],
                'progress': {
                    'map': {
                        'total': job_state['total_maps'],
                        'completed': job_state['completed_maps']
                    },
                    'reduce': {
                        'total': job_state['total_reduces'],
                        'completed': job_state['completed_reduces']
                    }
                }
            }
    
    def get_job_results(self, job_id):
        """
        Get the results of a completed job.
        
        Args:
            job_id: The job ID
            
        Returns:
            Dictionary with job results or None if not completed
        """
        with self.job_lock:
            if job_id not in self.job_state:
                return None
            
            job_state = self.job_state[job_id]
            
            if job_state['status'] != 'completed':
                return None
            
            return job_state['results']
    
    # Built-in mappers and reducers
    
    def _mapper_word_count(self, key, value):
        """
        Built-in mapper for word counting.
        
        Args:
            key: Document ID/name
            value: Document text
            
        Returns:
            List of (word, 1) pairs
        """
        words = value.lower().split()
        return [(word, 1) for word in words]
    
    def _reducer_word_count(self, key, values):
        """
        Built-in reducer for word counting.
        
        Args:
            key: The word
            values: List of counts (all 1's from mapper)
            
        Returns:
            Total count for the word
        """
        return sum(values)
    
    def _mapper_chat_analysis(self, key, value):
        """
        Mapper for chat message analysis.
        
        Args:
            key: Message ID
            value: Chat message object
            
        Returns:
            Analysis tuples
        """
        results = []
        
        # User activity (user_id -> 1)
        results.append((f"user:{value['user_id']}", 1))
        
        # Room activity (room_id -> 1)
        results.append((f"room:{value['room_id']}", 1))
        
        # Word count
        if 'content' in value:
            words = value['content'].lower().split()
            for word in words:
                results.append((f"word:{word}", 1))
        
        # Time-based activity
        if 'timestamp' in value:
            # Hourly activity (hour -> 1)
            hour = time.strftime("%H", time.localtime(value['timestamp']))
            results.append((f"hour:{hour}", 1))
            
            # Daily activity (day -> 1)
            day = time.strftime("%Y-%m-%d", time.localtime(value['timestamp']))
            results.append((f"day:{day}", 1))
        
        return results
    
    def _reducer_chat_analysis(self, key, values):
        """
        Reducer for chat message analysis.
        
        Args:
            key: The analysis key (e.g., "user:123", "word:hello")
            values: List of counts
            
        Returns:
            Total count for the key
        """
        return sum(values)
    
    def analyze_chat_data(self):
        """
        Run a MapReduce job to analyze chat data.
        
        Returns:
            Job ID
        """
        # Get all chat messages
        messages = self.data_store.get_all('chat_messages')
        
        # Generate a job ID
        job_id = f"chat_analysis_{int(time.time())}"
        
        # Run the job
        return self.run_job(
            job_id,
            'chat_analysis',  # Using built-in mapper
            'chat_analysis',  # Using built-in reducer
            messages
        )
