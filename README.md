# Apache Beam Pipelines – Batch & Streaming Projects

This repository contains educational projects built with **Apache Beam** in **Python**, designed to demonstrate key concepts of **batch** and **streaming** data processing. The pipelines leverage **Google Cloud Platform** services such as:

-  **Google Cloud Pub/Sub**
-  **Google Cloud Storage**
-  **Google Dataflow**

These small-scale yet practical exercises are intended for learning how to process large-scale data using modern stream-processing architectures.

---

## 📁 Project Overview

### Bank Data
- **Type:** Batch Processing  
- **Description:** Filtering customer data based on various criteria.  
- **Objective:** Understand batch pipeline fundamentals and learn basic data transformations using Beam.

---

### Streaming Data Generator
- **Type:** Streaming  
- **Description:** A simple tool to simulate streaming data using files from **Google Cloud Storage** and publish them to **Google Pub/Sub**.  
- **Objective:** Practice simulating real-time data for development and testing of streaming pipelines.

---

### Store Data
- **Type:** Streaming  
- **Description:** Streaming data pipeline applying **windows**, **triggers**, and **accumulation modes** to incoming data.  
- **Objective:** Learn core concepts of stream windowing and trigger mechanisms in Apache Beam.

---

### Mobile Game
- **Type:** Streaming  
- **Description:** Analyzing mobile game event streams to:  
  - Identify the most skilled weapon per player.  
  - Update real-time scores.  
- **Objective:** Practice complex stateful stream processing and user-level analytics.

---

## Technologies Used
- **Apache Beam** (Python SDK)
- **Google Cloud Pub/Sub**
- **Google Cloud Storage**
- **Google Dataflow**
- **Python 3.12**

---

## Getting Started

1. Enable the following Google Cloud APIs:
   - Pub/Sub
   - Dataflow
   - Cloud Storage

2. Set up authentication with:
   ```bash
   gcloud auth application-default login
Install dependencies:

```
pip install apache-beam[gcp]
```

Run a pipeline (example for Dataflow):

```
python your_pipeline.py \
  --runner DataflowRunner \
  --project YOUR_PROJECT_ID \
  --temp_location gs://YOUR_BUCKET/temp \
  --region YOUR_REGION
```

### Learning Goals
Understand differences between batch and streaming processing.

Learn Pub/Sub-based architectures.

Explore windowing, triggers, and stateful processing.

Practice building and deploying scalable pipelines on Google Cloud Dataflow.

### TODO
- Add metrics and monitoring (e.g., Stackdriver integration).

- Visualize pipeline performance and outputs.

 - Expand streaming examples with side inputs / outputs.

 - Integrate with BigQuery for final sinks.

 - Add Terraform scripts for infrastructure provisioning.

📚 References
Apache Beam Documentation

GCP Dataflow Quickstart

Pub/Sub Concepts
