# 🐍 Snake Venom Transcriptomics — Parallel & Distributed Computing Pipeline

> **PDC Final Lab Exam** | Framework: Python `multiprocessing` + Apache PySpark  
> **Dataset:** UniProt Snake Venom Toxin Proteins (FASTA) — ~500 sequences, scaled to 25,000+ for benchmarking

---

## 👥 Team Members

| Name | Role |
|------|------|
| Member 1 | Data ingestion, parallel feature extraction, PySpark pipeline |
| Member 2 | Statistical analysis, clustering (KMeans + PCA), visualizations |

> *(Replace with actual names)*

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

---

## 🚀 Setup & Run Instructions

### Option A — Google Colab (Recommended)

1. Open [Google Colab](https://colab.research.google.com)
2. Upload `01_Code/colab_venom.py` or paste it into a notebook
3. Run all cells top to bottom — all dependencies are installed inside the notebook

### Option B — Local Environment

#### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/PDC_FinalLab_SnakeVenomTranscriptomics.git
cd PDC_FinalLab_SnakeVenomTranscriptomics
```

#### 2. Create and activate a virtual environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

#### 3. Install dependencies

```bash
pip install pyspark biopython requests pandas numpy matplotlib seaborn scikit-learn
```

#### 4. Run the pipeline

```bash
python 01_Code/colab_venom.py
```

> **Note:** Java is required for PySpark. Install [Java 11+](https://adoptium.net/) and ensure `JAVA_HOME` is set before running locally.

---

## 📊 Results Summary

### ⚡ Parallel Speedup Benchmark

| Mode | Dataset Size | Time (s) | Speedup |
|------|-------------|----------|---------|
| Serial | 25,000 proteins | ~X.XXX s | 1.00× |
| Parallel (2–4 cores) | 25,000 proteins | ~X.XXX s | ~1.5–2×+ |

> Actual values printed at runtime. Speedup scales with dataset size and available cores.

### 🔬 PySpark Species-Level Analysis (Top Results)

| Species | # Proteins | Avg Length (AA) | Avg Cysteine Freq |
|---------|-----------|-----------------|-------------------|
| Naja naja | ... | ... | ... |
| Crotalus adamanteus | ... | ... | ... |
| Ophiophagus hannah | ... | ... | ... |

> *(Populated automatically from `data/species_summary.csv` after running the pipeline)*

### 🧠 KMeans Cluster Biological Interpretation

| Cluster | Likely Protein Type | Characteristics |
|---------|-------------------|-----------------|
| **Cluster 0** | 3-finger toxins / Neurotoxins | Short sequences, **high cysteine** frequency — common in *Naja* (cobras) |
| **Cluster 1** | Phospholipase A2 / Serine proteases | Long sequences, **low cysteine** — typical of pit vipers (*Crotalus*) |
| **Cluster 2/3** | Metalloproteinases / Multi-domain enzymes | Mixed length, moderate cysteine content |

---

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
