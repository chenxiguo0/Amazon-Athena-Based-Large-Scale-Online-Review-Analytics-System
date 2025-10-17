# Amazon Athena-Based Large Scale Online Review Analytics System

——— Large-Scale Reddit Comment Analysis with Amazon Athena & AWS Glue

## Project Overview

This project focuses on leveraging Amazon Athena, a serverless query service, and AWS Glue Data Catalog to perform extensive analysis on a large dataset of Reddit comments. It demonstrates proficiency in cloud-based data querying, schema management, and extracting meaningful insights from social media data using standard SQL. The project also includes developing an advanced data pipeline for extracting and transforming AI/GenAI-related comments for potential LLM training.

## Key Technologies & Skills Demonstrated

*   **Cloud Data Analytics:** Amazon Athena, AWS Glue Data Catalog
*   **Data Storage:** AWS S3 (for data lake architecture)
*   **Query Language:** SQL (complex queries, `REGEXP_LIKE`, aggregations, window functions)
*   **Data Processing:** ETL principles, Python (boto3, Polars for data manipulation)
*   **Data Engineering:** Designing and implementing a data pipeline for LLM training data preparation.
*   **Data Analysis:** Exploratory Data Analysis, temporal analysis, text pattern matching.
*   **Reporting:** CSV report generation.
*   **Cloud Infrastructure:** AWS EC2, AWS CLI, IAM permissions, Security Groups.

## Project Architecture & Data Pipeline Setup

The project leverages a robust cloud-native architecture for efficient data processing:

*   **Data Source:** Reddit comments for June 2023 (Parquet format), stored in a shared Amazon S3 bucket (`s3://dsan6000-datasets/reddit/`).
*   **Data Ingestion:** A script was developed to copy a subset of this data (~10 Parquet files) to a dedicated project-specific S3 bucket, optimizing for query performance and cost.
*   **Schema Discovery & Cataloging:** AWS Glue Data Catalog was configured, and an AWS Glue Crawler was used to automatically discover the schema of the Parquet files in S3, creating a queryable table (`a05` within the `a05` database) for Athena.
*   **Query Engine:** Amazon Athena served as the primary interactive query engine, allowing for ad-hoc and complex SQL queries directly on data in S3 without server management.

**Key Configuration Steps:**
*   Provisioned an AWS EC2 instance as a development environment.
*   Configured AWS CLI and Python environment with necessary libraries (boto3, pandas, ipykernel).
*   Established an AWS Glue Data Catalog and Crawler with appropriate IAM roles for S3 access and schema inference.
*   Verified Athena setup by executing a `COUNT(*)` query on the cataloged Reddit data.

## Core Data Analysis & Insights

Utilizing Amazon Athena, I performed a series of analytical queries on the Reddit comments data to extract key insights:

1.  **Top Active Subreddits:** Identified the top 10 subreddits with the highest comment counts, providing insight into community engagement. (Output: `prob1_results.csv`)
2.  **Data Schema Exploration:** Sampled 10 random comment entries to understand the dataset's structure and available fields. (Output: `prob2_results.csv`)
3.  **Temporal Comment Patterns:** Analyzed comment volume per day and hour to identify peak activity periods. (Output: `prob3_results.csv`)
4.  **Highest Rated Subreddits:** Determined the top 10 subreddits based on the highest average comment score, indicating high-quality content or community appreciation. (Output: `prob4_results.csv`)
5.  **Controversial Discussions in r/datascience:** Identified the top 5 most controversial comments within the 'datascience' subreddit, focusing on comments with high controversy scores. (Output: `prob5_results.csv`)

Each analysis involved crafting optimized SQL queries within Athena and generating corresponding CSV reports.

## Advanced Data Pipeline: AI/GenAI Comment Extraction for LLM Training

A significant component of this project involved developing an advanced data pipeline to extract and transform high-quality, AI/GenAI-related Reddit comments, suitable for training Large Language Models (LLMs). This demonstrates an understanding of ETL processes for AI/ML applications.

### Phase A: SQL-based Feature Engineering

*   **Objective:** Developed a comprehensive SQL query to filter and extract relevant comments based on specific criteria.
*   **Approach:** Used `REGEXP_LIKE` for advanced pattern matching on subreddit names (e.g., `artificial`, `chatgpt`, `openai`, `machinelearning`) and comment body.
*   **Quality Filtering:** Applied strict quality thresholds including:
    *   Comment length (100-1000 characters)
    *   Minimum score (>= 2)
    *   Exclusion of deleted/removed comments.
*   **Output:** Generated `prob6_ai_comments.csv` containing the filtered AI/GenAI comments.

### Phase B: Python-based ETL Pipeline Development

*   **Objective:** Implemented a Python script (`athena_to_jsonl_pipeline.py`) to programmatically execute the Athena query, process the results, and transform them into a format suitable for LLM training.
*   **Key Pipeline Steps:**
    *   Programmatically executed Athena queries using `boto3`.
    *   Downloaded query results from S3.
    *   Leveraged `Polars` for efficient in-memory data loading and manipulation.
    *   Performed data cleaning (e.g., removal of URLs, mentions, excessive whitespace).
    *   Created contextual instruction-response pairs from comments.
    *   Added relevant metadata (score, subreddit, timestamp).
    *   Filtered out any remaining low-quality content.
    *   Exported the final processed data into `JSONL` format, a standard for LLM training datasets.
*   **Output:** Produced `prob6_training_data.jsonl`, a refined dataset ready for LLM fine-tuning.

## Repository Structure

```
project-amazon-athena/
├── README.md                      # Project overview and detailed description
├── athena.ipynb                   # Jupyter notebook with all SQL queries and Python code for analysis
├── athena_to_jsonl_pipeline.py    # Python script for the advanced LLM training data pipeline
├── pipeline_concepts_explained.md # (Optional: Explanation of pipeline concepts, if provided in original assignment)
├── prob1_results.csv              # Output: Top 10 subreddits by comment count
├── prob2_results.csv              # Output: 10 random comment rows
├── prob3_results.csv              # Output: Comments per day per hour
├── prob4_results.csv              # Output: Top 10 subreddits by average score
├── prob5_results.csv              # Output: Top 5 controversial comments in r/datascience
├── prob6_ai_comments.csv          # Output: Extracted AI/GenAI comments (intermediate)
└── prob6_training_data.jsonl      # Output: Final LLM training data in JSONL format
```

## Setup & Usage

To set up the AWS environment, ingest data, and execute the analysis:

1.  **Prerequisites:**
    *   AWS account with permissions for Athena, S3, Glue, and EC2.
    *   AWS CLI configured.
    *   Python 3.10+ with `uv` (or `pip`).
    *   An EC2 instance (t3.medium or larger recommended) for development.

2.  **Clone the Repository & Environment Setup:**
    ```bash
    git clone https://github.com/your-username/your-repo-name.git # Replace with your actual repo URL
    cd your-repo-name
    uv sync # Install Python dependencies
    source .venv/bin/activate # Activate virtual environment
    uv pip install ipykernel
    python -m ipykernel install --user --name=athena-project # Install Jupyter kernel
    ```
    *Ensure you select the `athena-project` kernel in Jupyter/VSCode.*

3.  **Data Ingestion to S3:**
    *   Replace `<your-net-id>` with your AWS identifier in the script.
    *   Run the following commands to create an S3 bucket and copy the Reddit data (refer to the original `README.md` for the full loop of copying 10 parquet files):
    ```bash
    NETID=<your-net-id>
    BUCKET=athena-$NETID
    aws s3 mb s3://$BUCKET
    for i in {1..10}; do
        echo "Processing file $i of 10..."
        aws s3 cp s3://dsan6000-datasets/reddit/parquet/comments/yyyy=2023/mm=06/comments_RC_2023-06.zst_${i}.parquet ./comments_RC_2023-06.zst_${i}.parquet --request-payer
        aws s3 cp ./comments_RC_2023-06.zst_${i}.parquet s3://$BUCKET/data/a05/
        rm ./comments_RC_2023-06.zst_${i}.parquet
    done
    aws s3 ls s3://$BUCKET/data/a05/
    ```

4.  **AWS Glue Data Catalog & Athena Setup:**
    *   In your AWS console, create an AWS Glue database named `a05`.
    *   Configure an AWS Glue Crawler to crawl the S3 path `s3://$BUCKET/data/a05/`, ensuring it uses an appropriate IAM role (e.g., `LabRole`).
    *   Run the crawler to automatically discover the schema and create the `a05` table in the `a05` database.
    *   Configure Amazon Athena to use `s3://$BUCKET/athena-query-results/` for query output.
    *   Verify setup with a simple `SELECT count(*) FROM "AwsDataCatalog"."a05"."a05";` query in Athena.

5.  **Execute Analysis & Pipeline:**
    *   Open `athena.ipynb` in Jupyter/VSCode, select the `athena-project` kernel, and run all cells to perform the core data analyses (Tasks 1-5).
    *   For the advanced LLM training data pipeline (Task 6), execute the Python script:
    ```bash
    uv run python athena_to_jsonl_pipeline.py --net-id <your-net-id>
    ```

6.  **Cleanup:**
    *   Remember to delete your S3 bucket and any EC2 instances/Glue crawlers to avoid incurring unnecessary AWS costs.

## License

MIT License
