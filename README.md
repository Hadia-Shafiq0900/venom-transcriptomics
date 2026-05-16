# 🐍 Snake Venom Transcriptomics — Parallel & Distributed Computing Pipeline

> **PDC Final Lab Exam** | Framework: Python `multiprocessing` + Apache PySpark  
> **Dataset:** UniProt Snake Venom Toxin Proteins (FASTA) — ~500 sequences, scaled to 25,000+ for benchmarking

---



## 📌 Problem Statement

Snake venom proteomes contain hundreds of toxic proteins that differ in amino acid composition, cysteine-rich motifs, and sequence length — all of which determine venom toxicity. This project builds a **distributed bioinformatics pipeline** to:

1. Download and parse snake venom protein sequences from UniProt
2. Extract biochemical features (amino acid frequencies, motif counts) in **serial vs. parallel** to benchmark speedup
3. Load features into **Apache PySpark** for distributed groupBy analytics across species
4. Apply **KMeans clustering + PCA** to group toxin proteins by biochemical similarity
5. Produce biological interpretations of each cluster

---

## 🗂️ Repository Structure

```
PDC_FinalLab_SnakeVenomTranscriptomics/
│
├── 01_Code/
│   └── colab_venom.py              # Full pipeline (Person 1 + Person 2)
│
├── 02_Data_Notes/
│   └── dataset_info.md             # Source, format, size, preprocessing details
│
├── 03_Results/
│   ├── speedup_plot.png            # Serial vs Parallel bar chart
│   ├── species_barplot.png         # Top 10 species by protein count
│   ├── aa_heatmap.png              # Amino acid frequency heatmap
│   ├── species_comparison.png      # Normalized metrics for top 5 species
│   ├── elbow_plot.png              # KMeans elbow curve
│   └── pca_clusters.png            # PCA scatter plot with cluster labels
│
├── 04_Video_Link/
│   └── video_link.txt              # YouTube / Drive link to explainer video
│
└── README.md                       # This file
```

---

## 🧬 Dataset

| Field | Details |
|-------|---------|
| **Source** | [UniProt REST API](https://rest.uniprot.org) |
| **Query** | `taxonomy: Serpentes AND (keyword: toxin OR keyword: venom)` |
| **Format** | FASTA |
| **Raw size** | ~500 sequences |
| **Scaled size** | ~25,000 sequences (50× replication for benchmarking) |
| **Key fields** | Protein ID, Species name, Amino acid sequence |

---

## ⚙️ Pipeline Overview

```
UniProt API
    │
    ▼
[1] Data Ingestion          → Download FASTA, parse with BioPython → raw_sequences.csv
    │
    ▼
[2] Feature Extraction      → AA frequencies (20 AAs) + motif counts (RGD, CXXC, CXCX)
    │   ├── Serial (1 core)
    │   └── Parallel (multiprocessing.Pool, 4 cores)  ← speedup benchmark
    │
    ▼
[3] PySpark Distributed Analysis
    │   ├── createDataFrame → repartition(8) → cache()
    │   ├── MAP: protein_id + cysteine frequency
    │   ├── FILTER: seq_length > 50 AA
    │   └── GROUPBY species → avg_length, avg_cysteine, max_length, min_length
    │
    ▼
[4] ML Analysis (Person 2)
    │   ├── StandardScaler normalization
    │   ├── KMeans clustering (K=4, chosen via elbow method)
    │   └── PCA (2 components) for cluster visualization
    │
    ▼
[5] Results & Biological Interpretation
        └── Plots, summary tables, cluster profiles
```
# PART 1 — DATA PROCESSING PIPELINE

## Step 1 — Import Libraries

```python
import numpy as np
import pandas as pd
from multiprocessing import Pool
from Bio import SeqIO
```

### What this does

You imported all required libraries.

### Important libraries

| Library | Purpose |
|---|---|
| pandas | Handle tables/dataframes |
| numpy | Numerical operations |
| multiprocessing | Parallel CPU processing |
| BioPython | Read FASTA biological files |
| matplotlib | Graph plotting |
| pyspark | Distributed computing |

---

## Step 2 — Install Dependencies

```python
!pip install pyspark biopython requests
```

### What this does

Installs required packages in Google Colab.

### Why needed

Google Colab does not contain all bioinformatics libraries by default.

You installed:

- PySpark
- BioPython
- Requests

---

## Step 3 — Create Folders

```python
os.makedirs("data", exist_ok=True)
os.makedirs("03_Results", exist_ok=True)
```

### What this does

Creates directories to store:

- datasets
- output CSVs
- graphs
- results

---

## Step 4 — Download Dataset from UniProt

```python
response = requests.get(url)
```

### What this does

Downloads venom-related snake protein sequences from UniProt API.

The query searches:

- Snake proteins
- Venom/toxin proteins

Returned format:

- FASTA

---

## Step 5 — Save FASTA File

```python
with open("data/snake_venoms.fasta", "w") as f:
    f.write(response.text)
```

### What this does

Saves downloaded protein sequences locally.

---

## Step 6 — Parse FASTA Sequences

```python
for record in SeqIO.parse(...):
```

### What this does

Reads each protein sequence one-by-one.

From every protein you extracted:

| Feature | Meaning |
|---|---|
| protein_id | Unique protein ID |
| species | Snake species |
| sequence | Amino acid sequence |

Then stored everything in a dataframe.

---

## Step 7 — Convert to Pandas DataFrame

```python
df_raw = pd.DataFrame(records)
```

### What this does

Converts extracted protein information into table format.

### Example

| protein_id | species | sequence |
|---|---|---|
| P12345 | Naja naja | MKLLL... |

---

## Step 8 — Feature Extraction Function

```python
def extract_features(row):
```

### This is the MOST IMPORTANT function in the project.

### Inside the Function

#### A. Sequence Length

```python
L = len(seq)
```

Calculates protein length.

#### B. Amino Acid Frequencies

```python
freq = {aa: seq.count(aa)/L}
```

Calculates frequency of amino acids.

### Example

| Amino Acid | Meaning |
|---|---|
| C | Cysteine |
| A | Alanine |
| R | Arginine |

#### C. Motif Detection

### RGD motif

```python
seq.count("RGD")
```

Biologically important motif.

### CXXC motif

```python
seq[i] == "C" and seq[i+3] == "C"
```

Detects cysteine patterns.

### CXCX motif

Another cysteine-rich pattern.

Important because venom proteins are usually cysteine-rich.

---

## Step 9 — Serial Processing

```python
serial_results = [extract_features(row) for row in rows]
```

### What this does

Processes proteins ONE-BY-ONE using single CPU core.

This is baseline performance.

---

## Step 10 — Parallel Processing

```python
with Pool(processes=4) as pool:
```

### What this does

Uses multiple CPU cores simultaneously.

Instead of:

- 1 protein at a time

it processes:

- multiple proteins together

### Why this matters

This is the MAIN PDC concept.

You demonstrated:

- multi-core computing
- parallel execution
- workload distribution

---

## Step 11 — Speedup Calculation

```python
speedup = serial_time / parallel_time
```

### What this does

Measures improvement from parallelization.

### Example

| Mode | Time |
|---|---|
| Serial | 10 sec |
| Parallel | 4 sec |

### Speedup

```python
10 / 4 = 2.5x
```

---

## Step 12 — Artificial Dataset Scaling

```python
df_large = pd.concat([df_raw] * 50)
```

### What this does

Copies dataset 50 times.

### Why?

Because original dataset (~500 proteins) is too small for real distributed computing demonstration.

After scaling:

```text
500 → 25,000 proteins
```

This simulates large-scale workload.

---

## Step 13 — Generate Speedup Plot

```python
plt.bar(labels, times)
```

### What this does

Creates graph comparing:

- Serial execution time
- Parallel execution time

This is one of your required result plots.

---

# PART 2 — SPARK DISTRIBUTED COMPUTING

## Step 14 — Start Spark Session

```python
SparkSession.builder
```

### What this does

Initializes Apache Spark engine.

Spark manages distributed processing.

### Important Configurations

```python
.config("spark.sql.shuffle.partitions", "8")
```

Defines partition count.

More partitions:

- better distribution
- better parallelism

---

## Step 15 — Convert Pandas → Spark DataFrame

```python
sdf = spark.createDataFrame(df_features)
```

### What this does

Moves data into Spark distributed format.

---

## Step 16 — Repartitioning

```python
sdf.repartition(8)
```

### What this does

Splits dataset into 8 partitions.

Each partition can process independently.

This is a CORE distributed computing concept.

---

## Step 17 — Caching

```python
sdf.cache()
```

### What this does

Stores dataframe in memory.

Prevents recomputation.

Improves performance.

---

## Step 18 — Partition Analysis

```python
mapPartitionsWithIndex()
```

### What this does

Shows how data is distributed across partitions.

Very important PDC demonstration.

---

## Step 19 — Spark MAP Operation

```python
sdf.rdd.map(...)
```

### What this does

Applies transformation on distributed records.

MAP is one of Spark's core operations.

---

## Step 20 — Spark FILTER Operation

```python
filter(col("seq_length") > 50)
```

### What this does

Keeps only long proteins.

Demonstrates distributed filtering.

---

## Step 21 — Spark GROUPBY Aggregation

```python
groupBy("species").agg(...)
```

### What this does

Computes species-level statistics.

### Example

| Species | Avg Length |
|---|---|
| Cobra | 75 |
| Viper | 120 |

---

## Step 22 — Export Results

```python
to_csv()
```

### What this does

Saves processed outputs.

### Generated files

| File | Purpose |
|---|---|
| processed_features.csv | Extracted features |
| species_summary.csv | Species statistics |
| parquet files | Distributed storage |

---

# PART 3 — VISUALIZATION & MACHINE LEARNING

## Step 23 — Load Data for Analysis

```python
pd.read_csv()
```

### What this does

Loads processed data again.

---

## Step 24 — Summary Statistics

Calculates:

- total proteins
- average lengths
- species counts

---

## Step 25 — Histogram

```python
plt.hist()
```

### What this does

Shows distribution of protein lengths.

---

## Step 26 — Species Bar Plot

### What this does

Shows species with highest protein counts.

---

## Step 27 — Heatmap

```python
sns.heatmap()
```

### What this does

Visualizes amino acid frequencies across species.

---

## Step 28 — KMeans Clustering

```python
KMeans(n_clusters=4)
```

### What this does

Groups proteins with similar properties.

### Why clustering?

To discover hidden biological patterns.

---

## Step 29 — PCA

```python
PCA(n_components=2)
```

### What this does

Reduces dimensions for visualization.

Because amino acid features are high-dimensional.

---

## Step 30 — PCA Cluster Plot

### What this does

Plots clusters in 2D space.

Helps visualize protein grouping.

---

## Step 31 — Biological Interpretation

Final section explains biological meaning of clusters.

### Example

| Cluster | Meaning |
|---|---|
| High cysteine | Neurotoxins |
| Long proteins | Enzymes |

This connects computational analysis to biology.

---

# MOST IMPORTANT THINGS YOUR TEACHER CARES ABOUT

These are the strongest parts of your project:

## 1. Parallel Processing

```python
Pool(processes=4)
```

---

## 2. Dataset Scaling

```python
pd.concat([df_raw] * 50)
```

---

## 3. Spark Distributed Operations

- repartition
- cache
- groupBy
- map
- filter

---

## 4. Visualization

You generated multiple meaningful plots.

---

## 5. Biological Interpretation

You didn't just compute numbers.

You explained biological meaning too.

That matters a lot.
## 🗃️ Output Files

| File | Description |
|------|-------------|
| `data/raw_sequences.csv` | Parsed FASTA → protein ID, species, sequence |
| `data/processed_features.csv` | Full feature matrix (20 AA freqs + motifs) |
| `data/species_summary.csv` | PySpark groupBy aggregation per species |
| `data/features_parquet/` | Parquet export of Spark DataFrame |
| `03_Results/speedup_plot.png` | Serial vs Parallel execution time bar chart |
| `03_Results/species_barplot.png` | Top 10 species by protein count |
| `03_Results/aa_heatmap.png` | Mean AA frequencies heatmap (top 10 species) |
| `03_Results/species_comparison.png` | Normalized metrics for top 5 species |
| `03_Results/elbow_plot.png` | Elbow curve for KMeans K selection |
| `03_Results/pca_clusters.png` | PCA scatter plot coloured by cluster |

---

## 🛠️ Technologies Used

| Tool | Purpose |
|------|---------|
| **Python 3.10+** | Core language |
| **BioPython** | FASTA parsing |
| **requests** | UniProt API download |
| **multiprocessing.Pool** | CPU-parallel feature extraction |
| **Apache PySpark** | Distributed DataFrame operations (map, filter, groupBy, cache) |
| **scikit-learn** | KMeans clustering, PCA, StandardScaler |
| **matplotlib / seaborn** | All visualizations |
| **pandas / numpy** | Data wrangling |

---

## 📋 Contribution Statement

| Member | Specific Contributions |
|--------|----------------------|
| **Member 1** | UniProt data download, BioPython FASTA parsing, `extract_features()` function, serial vs parallel benchmarking, PySpark session setup, repartition/cache/groupBy analytics, Parquet export |
| **Member 2** | Feature matrix loading, summary statistics, sequence length histogram, species bar chart, AA frequency heatmap, species comparison chart, KMeans elbow method, KMeans (K=4) clustering, PCA visualization, biological interpretation write-up |

---

## 📎 References

- UniProt REST API: https://rest.uniprot.org
- BioPython Documentation: https://biopython.org
- Apache Spark: https://spark.apache.org
- scikit-learn: https://scikit-learn.org

---

## 🎥 Explainer Video

📺 [Watch here](<insert-link>) *(1–2 min: problem → dataset → pipeline → results → contributions)*
