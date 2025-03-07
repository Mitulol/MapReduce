# MapReduce Framework in Python

## Overview
This project implements a distributed MapReduce framework in Python, inspired by Google's original MapReduce paper. The framework executes MapReduce jobs with distributed processing across multiple worker nodes, supporting execution on a cluster of computers such as AWS EMR, Google Dataproc, or Microsoft MapReduce.

### Learning Objectives
- Understanding the MapReduce paradigm.
- Implementing basic distributed systems.
- Managing fault tolerance in a distributed setting.
- Utilizing OS-provided concurrency (threads and processes).
- Establishing networking through TCP and UDP sockets.

## Features
- **Distributed Processing**: Executes MapReduce tasks across multiple worker nodes.
- **Fault Tolerance**: Monitors worker nodes with heartbeat signals and reassigns tasks if a worker fails.
- **Dynamic Task Allocation**: Assigns map and reduce tasks to workers dynamically.
- **Efficient Data Partitioning**: Maps input files into partitions and distributes them across workers.
- **Logging & Debugging Support**: Provides detailed logs for execution tracking.

## Project Structure
```
mapreduce/
├── manager/        # Manager node implementation
│   ├── __init__.py
│   ├── __main__.py
├── worker/         # Worker node implementation
│   ├── __init__.py
│   ├── __main__.py
├── utils/          # Shared utilities
│   ├── __init__.py
│   ├── network.py  # TCP & UDP networking utilities
│   ├── ordered_dict.py # Thread-safe ordered dictionary
├── tests/          # Unit tests and sample input data
```

## Installation & Setup
### Prerequisites
- Python 3.8+
- pip

### Setup Instructions
1. Clone the repository:
   ```sh
   git clone <repository-url>
   cd mapreduce-framework
   ```
2. Create and activate a Python virtual environment:
   ```sh
   python3 -m venv env
   source env/bin/activate  # On Windows use `env\Scripts\activate`
   ```
3. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

## Running the Framework
### Starting the Manager and Workers
1. Start the Manager:
   ```sh
   python -m mapreduce.manager --host localhost --port 6000
   ```
2. Start Worker nodes in separate terminals:
   ```sh
   python -m mapreduce.worker --host localhost --port 6001 --manager-host localhost --manager-port 6000
   python -m mapreduce.worker --host localhost --port 6002 --manager-host localhost --manager-port 6000
   ```

### Submitting a MapReduce Job
Submit a job with the following command:
```sh
python -m mapreduce.submit --input tests/testdata/input_small --output output --mapper tests/testdata/exec/wc_map.sh --reducer tests/testdata/exec/wc_reduce.sh
```

### Checking the Output
Once the job completes, view the results:
```sh
cat output/part-*
```

## Fault Tolerance & Heartbeat Monitoring
- Workers send heartbeat messages to the Manager every 2 seconds.
- If a worker fails to send 5 consecutive heartbeats, it is marked as dead.
- Any tasks assigned to a dead worker are reassigned to available workers.
- The Manager ensures that tasks are not duplicated across workers.

## Testing
Run unit tests using `pytest`:
```sh
pytest -vvsx --log-cli-level=INFO
```

## Flowchart
The following flowchart illustrates the MapReduce execution process:
![MapReduce Flowchart](flowchart.png)

---
This project showcases a functional and fault-tolerant MapReduce framework, demonstrating core distributed system principles applicable to large-scale data processing.

