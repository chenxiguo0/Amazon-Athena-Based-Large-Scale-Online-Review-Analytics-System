#!/usr/bin/env python3
"""
athena_to_jsonl_pipeline.py

Complete pipeline for Problem 6:
1. Execute Athena query to extract AI/GenAI comments
2. Download query results from S3
3. Transform Reddit comments to JSONL for LLM training

This script:
- Executes SQL queries on Amazon Athena
- Polls for query completion
- Downloads results from S3
- Cleans and preprocesses the text using Polars
- Creates instruction-response pairs
- Exports to JSONL format for LLM fine-tuning

Author: Assignment Solution
Date: 2024
"""

import argparse
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
import polars as pl
from botocore.exceptions import ClientError


# Configure logging with basicConfig
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)


# Constants
DEFAULT_DATABASE = "reddit"
DEFAULT_TABLE = "a05"
DEFAULT_REGION = "us-east-1"
ATHENA_OUTPUT_LOCATION = "s3://{bucket}/data/results/"
QUERY_TIMEOUT = 300  # 5 minutes


def _get_athena_client(region: str = DEFAULT_REGION):
    """
    Create and return an Athena client.

    Args:
        region: AWS region

    Returns:
        Boto3 Athena client
    """
    return boto3.client('athena', region_name=region)


def _get_s3_client(region: str = DEFAULT_REGION):
    """
    Create and return an S3 client.

    Args:
        region: AWS region

    Returns:
        Boto3 S3 client
    """
    return boto3.client('s3', region_name=region)


def _read_query_from_file(query_file: str) -> str:
    """
    Read SQL query from file.

    Args:
        query_file: Path to SQL file

    Returns:
        SQL query string
    """
    try:
        with open(query_file, 'r') as f:
            query = f.read()
        logging.info(f"Read query from {query_file}")
        return query
    except Exception as e:
        logging.error(f"Failed to read query file: {e}")
        raise


def _execute_athena_query(
    query: str,
    database: str,
    output_location: str,
    region: str = DEFAULT_REGION
) -> str:
    """
    Execute a query on Amazon Athena.

    Args:
        query: SQL query to execute
        database: Athena database name
        output_location: S3 location for query results
        region: AWS region

    Returns:
        Query execution ID
    """
    client = _get_athena_client(region)

    try:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )

        query_id = response['QueryExecutionId']
        logging.info(f"Started query execution with ID: {query_id}")
        return query_id

    except ClientError as e:
        logging.error(f"Failed to execute query: {e}")
        raise


def _wait_for_query_completion(
    query_id: str,
    region: str = DEFAULT_REGION,
    timeout: int = QUERY_TIMEOUT
) -> Dict:
    """
    Wait for Athena query to complete.

    Args:
        query_id: Query execution ID
        region: AWS region
        timeout: Maximum wait time in seconds

    Returns:
        Query execution details
    """
    client = _get_athena_client(region)

    start_time = time.time()

    while True:
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Query {query_id} timed out after {timeout} seconds")

        try:
            response = client.get_query_execution(QueryExecutionId=query_id)
            status = response['QueryExecution']['Status']['State']

            if status in ['SUCCEEDED']:
                elapsed_time = time.time() - start_time
                logging.info(f"Query {query_id} completed successfully in {elapsed_time:.2f} seconds")

                # Log query statistics
                stats = response['QueryExecution'].get('Statistics', {})
                if stats:
                    execution_time_ms = stats.get('EngineExecutionTimeInMillis', 0)
                    data_scanned = stats.get('DataScannedInBytes', 0)
                    data_scanned_mb = data_scanned / (1024 * 1024)
                    data_scanned_gb = data_scanned / (1024 * 1024 * 1024)

                    logging.info("\n=== Query Execution Statistics ===")
                    logging.info(f"Engine Execution Time: {execution_time_ms / 1000:.2f} seconds")
                    logging.info(f"Data Scanned: {data_scanned:,} bytes ({data_scanned_mb:.2f} MB / {data_scanned_gb:.3f} GB)")
                    logging.info(f"Total Wait Time: {elapsed_time:.2f} seconds")

                    if 'DataManifestLocation' in stats:
                        logging.info(f"Data Manifest Location: {stats['DataManifestLocation']}")
                    if 'TotalExecutionTimeInMillis' in stats:
                        total_time_ms = stats['TotalExecutionTimeInMillis']
                        logging.info(f"Total Execution Time: {total_time_ms / 1000:.2f} seconds")
                    if 'QueryQueueTimeInMillis' in stats:
                        queue_time_ms = stats['QueryQueueTimeInMillis']
                        logging.info(f"Query Queue Time: {queue_time_ms / 1000:.2f} seconds")
                    if 'ServiceProcessingTimeInMillis' in stats:
                        service_time_ms = stats['ServiceProcessingTimeInMillis']
                        logging.info(f"Service Processing Time: {service_time_ms / 1000:.2f} seconds")

                return response['QueryExecution']
            elif status in ['FAILED', 'CANCELLED']:
                reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                raise Exception(f"Query {query_id} failed with status {status}: {reason}")

            # Query still running
            logging.debug(f"Query {query_id} status: {status}")
            time.sleep(2)

        except ClientError as e:
            logging.error(f"Failed to get query status: {e}")
            raise


def _download_query_results(
    query_id: str,
    output_file: str,
    region: str = DEFAULT_REGION
) -> Tuple[str, int]:
    """
    Download query results from S3.

    Args:
        query_id: Query execution ID
        output_file: Local file path to save results
        region: AWS region

    Returns:
        Tuple of (path to downloaded file, number of rows)
    """
    athena_client = _get_athena_client(region)

    try:
        start_download = time.time()
        logging.info("Starting to download query results...")

        # Get query results
        results = []
        next_token = None
        batch_count = 0

        while True:
            batch_count += 1
            if next_token:
                response = athena_client.get_query_results(
                    QueryExecutionId=query_id,
                    NextToken=next_token,
                    MaxResults=1000  # Max allowed by Athena API
                )
            else:
                response = athena_client.get_query_results(
                    QueryExecutionId=query_id,
                    MaxResults=1000  # Max allowed by Athena API
                )

            # Skip header row in first batch
            if not next_token:
                rows = response['ResultSet']['Rows'][1:]  # Skip header
            else:
                rows = response['ResultSet']['Rows']

            batch_size = len(rows)
            for row in rows:
                data = [col.get('VarCharValue', '') for col in row['Data']]
                results.append(data)

            next_token = response.get('NextToken')

            # Log progress
            logging.debug(f"Batch {batch_count}: Retrieved {batch_size} rows, Total: {len(results)} rows")
            if batch_count % 5 == 0 or batch_size < 999:
                logging.info(f"  Progress: Downloaded {len(results):,} rows (Batch {batch_count}, size: {batch_size})")

            if not next_token:
                logging.info(f"  Download complete: Total of {len(results):,} rows retrieved")
                break

        # Get column names
        header_response = athena_client.get_query_results(
            QueryExecutionId=query_id,
            MaxResults=1
        )
        columns = [col.get('VarCharValue', '') for col in header_response['ResultSet']['Rows'][0]['Data']]

        # Create DataFrame and save to CSV
        df = pl.DataFrame(results, schema=columns)
        df.write_csv(output_file)

        download_time = time.time() - start_download
        file_size = Path(output_file).stat().st_size / (1024 * 1024)  # Size in MB

        logging.info("\n=== Download Statistics ===")
        logging.info(f"Downloaded {len(df):,} rows to {output_file}")
        logging.info(f"Download Time: {download_time:.2f} seconds")
        logging.info(f"File Size: {file_size:.2f} MB")
        logging.info(f"Download Rate: {file_size/download_time:.2f} MB/s")

        return output_file, len(df)

    except ClientError as e:
        logging.error(f"Failed to download query results: {e}")
        raise


def _clean_text(text: str) -> str:
    """
    Clean and preprocess Reddit comment text.

    Args:
        text: Raw comment text

    Returns:
        Cleaned text string
    """
    if not text:
        return ""

    # Remove URLs
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
    text = re.sub(r'www\.(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)

    # Remove subreddit mentions (r/subreddit or /r/subreddit)
    text = re.sub(r'/?r/[a-zA-Z0-9_]+', '', text)

    # Remove user mentions (u/username or /u/username)
    text = re.sub(r'/?u/[a-zA-Z0-9_]+', '', text)

    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text)

    # Remove leading/trailing whitespace
    text = text.strip()

    return text


def _create_instruction(subreddit: str, score: int) -> str:
    """
    Create contextual instruction based on subreddit and score.

    Args:
        subreddit: Name of the subreddit
        score: Comment score

    Returns:
        Instruction string for the training pair
    """
    quality = "highly upvoted" if score >= 10 else "well-received" if score >= 5 else "thoughtful"

    # Create varied instructions based on subreddit type
    ai_tool_subreddits = ['chatgpt', 'openai', 'midjourney', 'stablediffusion', 'dalle', 'claude', 'bard']
    technical_subreddits = ['machinelearning', 'deeplearning', 'tensorflow', 'pytorch', 'ml']
    discussion_subreddits = ['artificial', 'singularity', 'agi', 'aiart', 'aiethics']

    subreddit_lower = subreddit.lower() if subreddit else ""

    if subreddit_lower in ai_tool_subreddits:
        instruction = f"Write a {quality} comment for r/{subreddit} discussing experiences with AI tools and their practical applications"
    elif subreddit_lower in technical_subreddits:
        instruction = f"Provide a {quality} technical insight for r/{subreddit} about machine learning concepts or implementation"
    elif subreddit_lower in discussion_subreddits:
        instruction = f"Share a {quality} perspective on r/{subreddit} about the implications and future of artificial intelligence"
    else:
        instruction = f"Contribute a {quality} comment to r/{subreddit} about AI and its impact on technology"

    return instruction


def _validate_comment(row: Dict) -> bool:
    """
    Validate if a comment meets quality criteria.

    Args:
        row: Dictionary containing comment data

    Returns:
        True if comment meets criteria, False otherwise
    """
    # Check if required fields exist
    if not all(key in row for key in ['body', 'subreddit', 'score']):
        return False

    body = row.get('body', '')

    # Skip deleted/removed comments
    if body in ['[deleted]', '[removed]', None, '']:
        return False

    # Check length requirements (after cleaning)
    cleaned_body = _clean_text(body)
    if len(cleaned_body) < 50 or len(cleaned_body) > 2000:
        return False

    # Check for minimum word count
    word_count = len(cleaned_body.split())
    if word_count < 10:
        return False

    return True


def transform_to_jsonl(
    input_csv: str,
    output_jsonl: str
) -> None:
    """
    Transform Reddit comments CSV to JSONL training data.

    Args:
        input_csv: Path to input CSV file
        output_jsonl: Path to output JSONL file
    """
    logging.info(f"Starting transformation from {input_csv} to {output_jsonl}")

    # Load data using Polars
    try:
        df = pl.read_csv(input_csv)
        logging.info(f"Loaded {len(df)} comments from CSV")
    except Exception as e:
        logging.error(f"Failed to load CSV: {e}")
        raise

    # Convert to dictionary records for processing
    records = df.to_dicts()

    # Process and transform comments
    training_data = []
    skipped_count = 0

    for record in records:
        # Validate comment quality
        if not _validate_comment(record):
            skipped_count += 1
            continue

        # Clean the comment text
        cleaned_body = _clean_text(record.get('body', ''))

        # Handle score conversion
        try:
            score = int(record.get('score', 0))
        except (TypeError, ValueError):
            score = 0

        # Create instruction-response pair
        instruction = _create_instruction(
            record.get('subreddit', ''),
            score
        )

        # Create training example
        training_example = {
            "instruction": instruction,
            "response": cleaned_body,
            "metadata": {
                "score": score,
                "subreddit": record.get('subreddit', ''),
                "timestamp": record.get('created_utc', 0),
                "controversiality": record.get('controversiality', 0)
            }
        }

        training_data.append(training_example)

    logging.info(f"Processed {len(training_data)} valid comments, skipped {skipped_count}")

    # Write to JSONL file
    try:
        with open(output_jsonl, 'w', encoding='utf-8') as f:
            for example in training_data:
                json_line = json.dumps(example, ensure_ascii=False)
                f.write(json_line + '\n')

        logging.info(f"Successfully wrote {len(training_data)} training examples to {output_jsonl}")

    except Exception as e:
        logging.error(f"Failed to write JSONL file: {e}")
        raise

    # Print summary statistics
    logging.info("\n=== Transformation Summary ===")
    logging.info(f"Total input comments: {len(df)}")
    logging.info(f"Valid training examples: {len(training_data)}")
    logging.info(f"Skipped comments: {skipped_count}")
    if len(df) > 0:
        logging.info(f"Success rate: {len(training_data)/len(df)*100:.2f}%")

    # Analyze subreddit distribution
    subreddit_counts = {}
    for example in training_data:
        subreddit = example['metadata']['subreddit']
        subreddit_counts[subreddit] = subreddit_counts.get(subreddit, 0) + 1

    if subreddit_counts:
        logging.info("\n=== Top 10 Subreddits in Training Data ===")
        sorted_subreddits = sorted(subreddit_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        for subreddit, count in sorted_subreddits:
            logging.info(f"  r/{subreddit}: {count} comments")


def main():
    """
    Main entry point for the script.
    """
    parser = argparse.ArgumentParser(
        description="Execute Athena query and transform Reddit comments to JSONL for LLM training",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
    # Run complete pipeline with default settings
    uv run python athena_to_jsonl_pipeline.py --net-id aa1603-1702

    # Use custom query file
    uv run python athena_to_jsonl_pipeline.py --net-id aa1603-1702 --query problem6_query.sql

    # Skip Athena query and just transform existing CSV
    uv run python athena_to_jsonl_pipeline.py --transform-only --input prob6_ai_comments.csv

    # With debug logging
    uv run python athena_to_jsonl_pipeline.py --net-id aa1603-1702 --debug
"""
    )

    parser.add_argument(
        "--net-id",
        type=str,
        help="Your net ID (e.g., aa1603-1702) for S3 bucket name"
    )

    parser.add_argument(
        "--query",
        type=str,
        default="problem6_query.sql",
        help="Path to SQL query file (default: problem6_query.sql)"
    )

    parser.add_argument(
        "--database",
        type=str,
        default=DEFAULT_DATABASE,
        help=f"Athena database name (default: {DEFAULT_DATABASE})"
    )

    parser.add_argument(
        "--region",
        type=str,
        default=DEFAULT_REGION,
        help=f"AWS region (default: {DEFAULT_REGION})"
    )

    parser.add_argument(
        "--csv-output",
        type=str,
        default="prob6_ai_comments.csv",
        help="Path for CSV output from Athena (default: prob6_ai_comments.csv)"
    )

    parser.add_argument(
        "--jsonl-output",
        type=str,
        default="prob6_training_data.jsonl",
        help="Path for JSONL output (default: prob6_training_data.jsonl)"
    )

    parser.add_argument(
        "--transform-only",
        action="store_true",
        help="Skip Athena query and only perform transformation"
    )

    parser.add_argument(
        "--input",
        type=str,
        help="Input CSV file for transform-only mode"
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )

    args = parser.parse_args()

    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Handle transform-only mode
    if args.transform_only:
        if not args.input:
            parser.error("--input is required when using --transform-only")

        transform_to_jsonl(args.input, args.jsonl_output)
        return

    # Validate net-id is provided for Athena execution
    if not args.net_id:
        parser.error("--net-id is required for Athena query execution")

    # Construct S3 bucket and output location
    bucket = f"athena-{args.net_id}"
    output_location = ATHENA_OUTPUT_LOCATION.format(bucket=bucket)

    pipeline_start = time.time()

    logging.info(f"Starting Athena to JSONL pipeline")
    logging.info(f"Net ID: {args.net_id}")
    logging.info(f"S3 Bucket: {bucket}")
    logging.info(f"Output Location: {output_location}")

    try:
        # Step 1: Read query from file
        query = _read_query_from_file(args.query)

        # Step 2: Execute Athena query
        logging.info("\n=== Step 1: Executing Athena Query ===")
        query_start = time.time()
        query_id = _execute_athena_query(
            query=query,
            database=args.database,
            output_location=output_location,
            region=args.region
        )

        # Step 3: Wait for completion
        logging.info("\n=== Step 2: Waiting for Query Completion ===")
        execution_details = _wait_for_query_completion(
            query_id=query_id,
            region=args.region
        )
        query_time = time.time() - query_start

        # Step 4: Download results
        logging.info("\n=== Step 3: Downloading Results ===")
        download_start = time.time()
        csv_file, row_count = _download_query_results(
            query_id=query_id,
            output_file=args.csv_output,
            region=args.region
        )
        download_time = time.time() - download_start

        # Step 5: Transform to JSONL
        logging.info("\n=== Step 4: Transforming to JSONL ===")
        transform_start = time.time()
        transform_to_jsonl(csv_file, args.jsonl_output)
        transform_time = time.time() - transform_start

        pipeline_time = time.time() - pipeline_start

        # Print final summary
        logging.info("\n" + "="*50)
        logging.info("=== PIPELINE COMPLETED SUCCESSFULLY ===")
        logging.info("="*50)
        logging.info(f"\nResults Summary:")
        logging.info(f"  Query Results: {row_count:,} rows")
        logging.info(f"  CSV Output: {args.csv_output}")
        logging.info(f"  JSONL Output: {args.jsonl_output}")

        logging.info(f"\nTiming Summary:")
        logging.info(f"  Query Execution: {query_time:.2f} seconds")
        logging.info(f"  Result Download: {download_time:.2f} seconds")
        logging.info(f"  Data Transformation: {transform_time:.2f} seconds")
        logging.info(f"  Total Pipeline Time: {pipeline_time:.2f} seconds")

        # Add cost estimate if data scanned is available
        if execution_details and 'Statistics' in execution_details:
            stats = execution_details['Statistics']
            if 'DataScannedInBytes' in stats:
                data_gb = stats['DataScannedInBytes'] / (1024 * 1024 * 1024)
                # Athena charges $5 per TB scanned
                estimated_cost = (data_gb / 1024) * 5
                logging.info(f"\nCost Estimate:")
                logging.info(f"  Data Scanned: {data_gb:.3f} GB")
                logging.info(f"  Estimated Query Cost: ${estimated_cost:.6f} (at $5/TB)")

    except Exception as e:
        logging.error(f"\nPipeline failed after {time.time() - pipeline_start:.2f} seconds")
        logging.error(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()