# Assignment: Amazon Athena Reddit Comments Analysis

## Overview

In this assignment, you will analyze Reddit comments data using Amazon Athena, a serverless, interactive analytics service. Amazon Athena allows you to analyze petabytes of data where it lives using standard SQL without managing any infrastructure. You will work with Reddit comments from June 2023 to understand patterns in community discussions.

## Learning Objectives

By completing this assignment, you will:

1. Set up Amazon Athena to query data stored in S3
2. Create and configure an AWS Glue Data Catalog for Reddit comments data
3. Execute complex SQL queries on large datasets using Amazon Athena
4. Analyze Reddit comment patterns to extract meaningful insights
5. Generate CSV reports from query results

## Prerequisites

### AWS Environment:
- AWS account with access to Amazon Athena, S3, and AWS Glue
- IAM permissions for creating S3 buckets and running Glue crawlers

### Development Environment:
- Amazon EC2 instance (t3.medium or larger recommended)
- VSCode with Remote-SSH extension
- Python 3.10 or higher
- AWS CLI configured with credentials

### Python Environment:
- `uv` package manager for dependency management
- Python 3.10 or higher

### Install uv:
- **macOS/Linux:** `curl -LsSf https://astral.sh/uv/install.sh | sh`
- **Windows:** `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`

### Required Python Packages:
- boto3 (AWS SDK for Python)
- pandas (Data manipulation)
- ipykernel (Jupyter kernel)
- jupyter (Notebook interface)

## Environment Setup

### 1. Amazon EC2 Instance Setup:
1. Launch an EC2 instance (t3.medium or larger)
2. Configure security group for SSH access
3. Connect using VSCode Remote-SSH extension

### 2. Python Environment Setup:
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the assignment repository
git clone <your-repo-url>
cd assignment-athena

# Create virtual environment and install dependencies
uv sync

# Activate the virtual environment
source .venv/bin/activate
```

### 3. Jupyter Setup:
```bash
# Install Jupyter kernel for the virtual environment
uv pip install ipykernel
python -m ipykernel install --user --name=assignment-athena
```

**Important:** When opening the notebook file in Jupyter or VSCode, make sure to select the `assignment-athena` kernel that you just created.

## Getting the Data for This Assignment

### Data Preparation:

From your EC2 instance terminal, run the following commands to copy the Reddit Comments Dataset into your S3 bucket.

**Important:** Replace `<your-net-id>` with your actual net id (e.g., for net id aa1603, the bucket name will be `athena-aa1603`).

```bash
# Set your net ID (replace <your-net-id> with your actual net id)
NETID=<your-net-id>
BUCKET=athena-$NETID

# Create a new S3 bucket
aws s3 mb s3://$BUCKET

# Verify that the bucket is created
aws s3 ls s3://$BUCKET/

# Download Reddit comments data for June 2023 (10 files)
# Use a loop to download, upload, and clean up 10 files
for i in {1..10}; do
    echo "Processing file $i of 10..."

    # Download the file locally with request-payer option
    aws s3 cp s3://dsan6000-datasets/reddit/parquet/comments/yyyy=2023/mm=06/comments_RC_2023-06.zst_${i}.parquet ./comments_RC_2023-06.zst_${i}.parquet --request-payer

    # Upload to your account-specific bucket
    aws s3 cp ./comments_RC_2023-06.zst_${i}.parquet s3://$BUCKET/data/a05/

    # Delete the local file to save space
    rm ./comments_RC_2023-06.zst_${i}.parquet
done

# Verify all files are uploaded
aws s3 ls s3://$BUCKET/data/a05/
```

**Expected Output:**
```
2024-09-20 13:51:29   [file-size] comments_RC_2023-06.zst_1.parquet
2024-09-20 13:51:31   [file-size] comments_RC_2023-06.zst_2.parquet
2024-09-20 13:51:33   [file-size] comments_RC_2023-06.zst_3.parquet
2024-09-20 13:51:35   [file-size] comments_RC_2023-06.zst_4.parquet
2024-09-20 13:51:37   [file-size] comments_RC_2023-06.zst_5.parquet
2024-09-20 13:51:39   [file-size] comments_RC_2023-06.zst_6.parquet
2024-09-20 13:51:41   [file-size] comments_RC_2023-06.zst_7.parquet
2024-09-20 13:51:43   [file-size] comments_RC_2023-06.zst_8.parquet
2024-09-20 13:51:45   [file-size] comments_RC_2023-06.zst_9.parquet
2024-09-20 13:51:47   [file-size] comments_RC_2023-06.zst_10.parquet
```

## Setting up Amazon Athena & Glue

Follow the same Athena and Glue setup instructions as you did in the lab to setup a Glue crawler and create a table that you can query using Athena. A few important points to note:

1. **Database Name:** Name the database as `a05`. The table name would automatically be `a05` since we ingested the data in `s3://$BUCKET/data/a05/`.

2. **Data Source:** Point your Glue crawler to `s3://$BUCKET/data/a05/` where you uploaded the Reddit comments parquet file.

3. **Setup Steps:** Follow the exact same steps as shown in the Athena lab for:
   - Creating AWS Glue Data Catalog
   - Setting up Glue crawler with IAM role (LabRole)
   - Running the crawler to discover the schema
   - Configuring Athena query result location

### Verification Step

Once Athena is setup, run the following query to confirm that you have setup everything correctly:

```sql
SELECT count(*) from "AwsDataCatalog"."a05"."a05";
```

If everything is setup correctly, you should be able to run this query in the Athena console and see the total count of rows in the Reddit comments table.

Once the above query works, you are ready to solve the problems in this assignment.

### What to submit

You can use the notebook included in the lab as started code for this assignment. Submit the notebook as **`athena.ipynb`**. This should include code for both the problems listed below.

Besides the notebook also submit the following csv files:
- `prob1_results.csv`
- `prob2_results.csv`
- `prob3_results.csv`
- `prob4_results.csv`
- `prob5_results.csv`


> [!IMPORTANT]
> It would be advisable to first try out all the queries in the Athena console before putting them in the Jupyter notebook.
> This way you can quickly test out if the queries work or not and get to a correct working version of the query quicker.

## Problem 1: Get top 10 subreddits by count of comments (15 points)

The response should contain two columns: `subreddit` and `comment_count`.

Save the results in a file called `prob1_results.csv`. This needs to be committed to the repo.

## Problem 2: Get 10 random rows from the comments table (15 points)

This will help you understand the schema of the table which will be useful for other queries.

Save the results in a file called `prob2_results.csv`. This needs to be committed to the repo.

## Problem 3: Get number of comments per day per hour and sort the results in descending order of the count (20 points)

The response should contain the following 3 columns: `comment_date`, `comment_hour` and `comment_count`.

Save the results in a file called `prob3_results.csv`. This needs to be committed to the repo.

## Problem 4: Find top 10 subreddits by the highest average score and sort the results in descending order of the average score (20 points)

The response should contain the following columns: `subreddit` and `avg_score`.

Save the results in a file called `prob4_results.csv`. This needs to be committed to the repo.

## Problem 5: Top 5 most Controversial Comments in a r\datascience subreddit (20 points)

The response should contain the following columns: `author`, `body`, `score`, `controversiality`.

Save the results in a file called `prob5_results.csv`. This needs to be committed to the repo.

## Problem 6: Extract and Transform AI/GenAI Comments for LLM Training - Advanced Pipeline (20 points)

> [!NOTE]
> **Learning Objectives:** This problem teaches advanced Athena concepts including pagination, data pipeline development, and ETL processes for AI/ML applications. The solution code is provided in `athena_to_jsonl_pipeline.py` for you to study and understand. Read `pipeline_concepts_explained.md` to understand the technical concepts demonstrated in this pipeline.

> [!IMPORTANT]
> **Requirements:** You must run the pipeline and submit the output file `prob6_training_data.jsonl` to receive credit. The goal is to understand how production data pipelines work, not to write the code from scratch.

### Part A: SQL Query Development (5 points)

Create a comprehensive SQL query to extract high-quality comments from AI/GenAI related subreddits using `REGEXP_LIKE` pattern matching. Your query should identify comments from the following categories:

**AI/ML Category Examples:**
- Core AI: `artificial`, `artificialintelligence`, `machinelearning`, `deeplearning`
- GenAI Tools: `ChatGPT`, `openai`, `midjourney`, `stablediffusion`, `dalle`
- Technical: `tensorflow`, `pytorch`, `huggingface`, `localllama`
- Discussion: `singularity`, `agi`, `aiart`, `comfyui`

**Quality Thresholds:**
- Comment length: between 100 and 1000 characters
- Score: >= 2 (well-received comments)
- Exclude deleted/removed comments (body != '[deleted]' and body != '[removed]')

**Required SQL Query Structure:**
```sql
SELECT
    subreddit,
    author,
    body,
    score,
    created_utc,
    controversiality
FROM "AwsDataCatalog"."reddit"."a05"
WHERE
    -- Use REGEXP_LIKE to match AI-related subreddits
    (
        REGEXP_LIKE(LOWER(subreddit), 'artificial|ai|genai') OR
        REGEXP_LIKE(LOWER(subreddit), 'chatgpt|openai|gpt') OR
        -- Add more patterns here
    )
    AND LENGTH(body) BETWEEN 100 AND 1000  -- Quality threshold
    AND score >= 2                         -- Well-received comments
    AND body NOT IN ('[deleted]', '[removed]')
ORDER BY score DESC;
```

Save the query results as `prob6_ai_comments.csv`.

### Part B: Data Pipeline Execution (5 points)

Write a Python script that performs the complete pipeline:
1. **Execute the Athena query** from Part A programmatically using boto3
2. **Download query results** from S3
3. **Load the data** using Polars
4. **Clean and preprocess** the comments (remove URLs, mentions, excessive whitespace)
5. **Create instruction-response pairs** for LLM training
6. **Export the data** in JSONL format

**Required Script Features:**
- Use boto3 to execute Athena queries and download results
- Remove URLs and excessive whitespace from comments
- Create contextual instructions based on subreddit
- Add metadata (score, subreddit, timestamp)
- Filter out any remaining low-quality content

Save your Python script as `athena_to_jsonl_pipeline.py` and the output as `prob6_training_data.jsonl`.

**Running the script:**
```bash
# Execute complete pipeline
uv run python athena_to_jsonl_pipeline.py --net-id <your-net-id>

# Or if you already have the CSV from Athena console
uv run python athena_to_jsonl_pipeline.py --transform-only --input prob6_ai_comments.csv
```

**Expected JSONL Format:**
```json
{"instruction": "Write an insightful comment for r/ChatGPT discussing AI capabilities", "response": "...", "metadata": {"score": 10, "subreddit": "ChatGPT", "timestamp": 1685685609}}
```

## Grading Rubric

### Problem Breakdown (100 points total)
- **Problem 1:** Top 10 subreddits by comment count - 15 points
- **Problem 2:** Random sample of 10 rows - 15 points
- **Problem 3:** Comments per day per hour analysis - 20 points
- **Problem 4:** Top 10 subreddits by average score - 20 points
- **Problem 5:** Most controversial comments in r/datascience - 10 points
- **Problem 6:** Extract and Transform AI/GenAI Comments Pipeline - 20 points
  - Part A (Understanding SQL Query): 10 points
  - Part B (Running Pipeline & Submission): 10 points

### Grading Criteria for Problems 1-2 (15 points each):
- **Correct SQL Query:** 10 points
- **CSV Output:** 3 points
- **Code Documentation:** 2 points

### Grading Criteria for Problem 3-4 (20 points each):
- **Correct SQL Query:** 10 points - Query produces the expected results with proper syntax
- **Output Format:** 5 points - CSV file contains the correct columns and data format
- **Code Documentation:** 3 points - Notebook includes clear explanations and comments
- **Results Accuracy:** 2 points - Results are logical and properly formatted

### Grading Criteria for Problem 5 (10 points):
- **Correct SQL Query:** 6 points - Query produces the expected results with proper syntax
- **Output Format:** 2 points - CSV file contains the correct columns and data format
- **Code Documentation:** 2 points - Notebook includes clear explanations and comments

### Grading Criteria for Problem 6 (20 points):
- **Understanding the Pipeline:** 6 points - Read and understand the provided solution
- **Running the Pipeline Successfully:** 8 points - Execute athena_to_jsonl_pipeline.py
- **Submitting JSONL Output:** 6 points - Submit prob6_training_data.jsonl with valid content

**Total: 100 points**

## Submission Requirements

### Required Files

Ensure the following files are committed and pushed to your GitHub repository:

1. **Notebooks:**
   - `athena.ipynb` (completed with all outputs and documentation)

2. **Output Files:**
   - `prob1_results.csv`
   - `prob2_results.csv`
   - `prob3_results.csv`
   - `prob4_results.csv`
   - `prob5_results.csv`
   - `prob6_ai_comments.csv`
   - `prob6_training_data.jsonl`

3. **Python Scripts:**
   - `athena_to_jsonl_pipeline.py`

### Submission Process

1. Complete all 6 problems in the Athena notebook (Problems 1-5) and Python script (Problem 6)
2. Generate all required CSV files from your query results
3. Ensure all output files are properly formatted
4. Commit all changes to your repository:
   ```bash
   git add .
   git commit -m "Complete Amazon Athena Reddit comments analysis assignment"
   git push origin main
   ```
