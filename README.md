# Snake Venom Transcriptomics using Parallel and Distributed Computing (PDC)

## Project Overview
This project focuses on **Snake Venom Transcriptomics Analysis** using **Parallel and Distributed Computing (PDC)** concepts.  
The aim of the project is to process large-scale venom transcriptomic datasets efficiently using MPI-based parallelization techniques.

We implemented decomposition strategies, benchmarking, workload distribution, and performance analysis to improve execution efficiency for computationally intensive biological datasets.

---

# Team Members
- Hadia Shafiq
- Dua Qaiser
- Momina Assad
- Zainab Bilal

---

# Project Objectives
- Analyze snake venom transcriptomic datasets
- Perform preprocessing and transcript-level analysis
- Apply MPI-based parallel computing
- Reduce computational runtime
- Benchmark processor performance
- Study scalability and load balancing

---

# Technologies Used

| Tool | Purpose |
|---|---|
| Python | Main programming language |
| MPI4Py | MPI parallelization |
| Google Colab | Development environment |
| Linux/HPC | Parallel execution |
| NumPy | Numerical computations |
| Pandas | Data handling |
| Matplotlib | Visualization |
| BioPython | Sequence processing |

---

# Parallel Computing Concepts Used

## 1. Data Decomposition
The dataset was divided into chunks so that multiple processors could work simultaneously on different portions of the data.

### Advantages
- Faster execution
- Better resource utilization
- Reduced runtime
- Improved scalability

---

## 2. Functional Decomposition
Different processors performed different computational tasks such as:
- Preprocessing
- Filtering
- Feature extraction
- Analysis
- Benchmarking

---

## 3. Hybrid Decomposition
We combined:
- Data decomposition
- Functional decomposition

This improved:
- Load balancing
- Processor utilization
- Execution speed
- Computational efficiency

---

# PCAM Methodology

## Partition
The transcriptomic dataset was partitioned into chunks across processors.

## Communication
Processors exchanged intermediate information when required using MPI communication functions.

## Agglomeration
Smaller computational tasks were grouped together to reduce communication overhead.

## Mapping
Tasks were mapped efficiently onto available processors for balanced execution.

---

# Workflow

# Step 1 — Setup Environment

## Install MPI and Required Libraries

```bash
!apt-get install -y mpich
!pip install mpi4py
!pip install biopython pandas numpy matplotlib
```

### Purpose
These libraries were required for:
- MPI-based parallel execution
- Sequence analysis
- Data processing
- Visualization

---

# Step 2 — Import Libraries

```python
from mpi4py import MPI
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from Bio import SeqIO
import time
```

### Purpose
- `mpi4py` → Parallel computing
- `pandas` → Dataset handling
- `numpy` → Numerical operations
- `matplotlib` → Graph plotting
- `BioPython` → Transcriptomic sequence processing

---

# Step 3 — Initialize MPI

```python
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
```

### Purpose
- `comm` → MPI communication object
- `rank` → Processor ID
- `size` → Total number of processors

---

# Step 4 — Load Transcriptomic Dataset

```python
data = pd.read_csv("venom_dataset.csv")
```

### Purpose
The venom transcriptomics dataset was loaded for preprocessing and parallel analysis.

---

# Step 5 — Dataset Preprocessing

## Cleaning Data

```python
data.dropna(inplace=True)
```

## Removing Duplicate Entries

```python
data.drop_duplicates(inplace=True)
```

## Normalization

```python
data = (data - data.mean()) / data.std()
```

### Purpose
Preprocessing improves:
- Data quality
- Accuracy
- Computational efficiency

---

# Step 6 — Data Decomposition

```python
chunks = np.array_split(data, size)
local_chunk = comm.scatter(chunks, root=0)
```

### Purpose
The dataset was divided equally among processors using MPI Scatter.

Each processor worked independently on its assigned chunk.

---

# Step 7 — Parallel Processing

```python
start_time = MPI.Wtime()

local_result = local_chunk.describe()

end_time = MPI.Wtime()
local_time = end_time - start_time
```

### Purpose
Each processor:
- Performed local computations
- Processed its assigned transcriptomic data
- Measured execution time independently

---

# Step 8 — Gather Results

```python
results = comm.gather(local_result, root=0)
times = comm.gather(local_time, root=0)
```

### Purpose
Results from all processors were collected back to the root processor.

---

# Step 9 — Benchmarking

```python
if rank == 0:
    max_time = max(times)
    avg_time = sum(times) / len(times)

    print("Execution Times:", times)
    print("Maximum Time:", max_time)
    print("Average Time:", avg_time)
```

### Benchmarking Measures
- Execution time
- Average runtime
- Maximum runtime
- Processor efficiency
- Load balancing

---

# Step 10 — Visualization

```python
if rank == 0:
    plt.bar(range(size), times)
    plt.xlabel("MPI Rank")
    plt.ylabel("Execution Time (seconds)")
    plt.title("MPI Benchmarking")
    plt.show()
```

### Purpose
The graph visualized:
- Compute time for each MPI rank
- Load balancing efficiency
- Parallel performance

---

# Benchmarking Observations

- Workload was evenly distributed among processors
- Execution times were very similar
- Parallelization significantly reduced runtime
- Minimal processor imbalance was observed

---

# Repository Structure

```bash
Snake-Venom-Transcriptomics/
│
├── data/
│   ├── venom_dataset.csv
│
├── notebooks/
│   ├── venom_analysis.ipynb
│
├── scripts/
│   ├── mpi_pipeline.py
│
├── results/
│   ├── benchmarking_output.png
│
├── figures/
│   ├── graphs/
│
├── README.md
```

---

# How to Run the Project

## Clone Repository

```bash
git clone https://github.com/your-username/your-repository.git
cd your-repository
```

---

## Run on Google Colab

Upload:
- Dataset
- Notebook
- Supporting files

Then run all notebook cells sequentially.

---

## Run MPI Program

```bash
mpirun -np 4 python mpi_pipeline.py
```

### Explanation
- `-np 4` → Run with 4 processors
- `mpi_pipeline.py` → Main MPI program

---

# Results
The project demonstrated that:
- Parallel computing significantly improves performance
- Large transcriptomic datasets can be processed efficiently
- MPI enables scalable biological data analysis
- Benchmarking confirmed effective load balancing

---

# Future Improvements
- GPU acceleration
- Cloud-based distributed execution
- Deep learning integration
- Advanced toxin classification
- Real-time venom analysis systems

---

# References
- MPI Documentation
- BioPython Documentation
- Transcriptomics Research Papers
- Parallel Computing Literature

---

# License
This project is for educational and academic purposes only.

---

# Acknowledgements
Special thanks to our instructors and lab supervisors for their guidance and support throughout this project.
