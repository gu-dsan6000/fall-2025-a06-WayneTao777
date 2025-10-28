from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand
import sys
import os
import glob
import shutil

def move_single_csv(src_dir, dst_file):
    """Move Spark's part-*.csv to a single file and clean up temporary output folder."""
    part_files = glob.glob(os.path.join(src_dir, "part-*.csv"))
    if part_files:
        shutil.move(part_files[0], dst_file)
    # Remove Spark metadata and folder
    for extra in glob.glob(os.path.join(src_dir, "*")):
        try:
            os.remove(extra)
        except:
            pass
    try:
        os.rmdir(src_dir)
    except:
        pass

def main():
    # Parse command line arguments
    if len(sys.argv) < 3:
        print("Usage: python problem1.py <spark-master-url> --net-id <YOUR_NET_ID>")
        sys.exit(1)
    
    master_url = sys.argv[1]
    net_id = sys.argv[3] if len(sys.argv) >= 4 else "unknown"

    # Initialize Spark Session
    spark = (
        SparkSession.builder
        .appName("Problem1_LogLevelDistribution")
        .master(master_url)
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3a.connection.timeout", "5000")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    bucket = os.environ.get("SPARK_LOGS_BUCKET")
    if not bucket:
        raise EnvironmentError("SPARK_LOGS_BUCKET environment variable not found!")

    # Use s3a:// protocol (required for Spark)
    bucket = bucket.replace("s3://", "s3a://")
    s3_path = f"{bucket}/data/application_*/*"
    # Construct S3 path for input logs
    s3_path = f"s3://{net_id}-assignment-spark-cluster-logs/data/application_*/*"
    print(f"Reading logs from: {s3_path}")

    # Read all log files
    logs_df = spark.read.text(s3_path)

    # Extract structured fields using regex
    parsed_df = logs_df.select(
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('log_level'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
        col('value').alias('message')
    )

    # Filter valid log lines
    filtered_df = parsed_df.filter(col("log_level") != "")

    # Count each log level
    counts_df = filtered_df.groupBy("log_level").count().orderBy(col("count").desc())

    # Prepare sample (10 random entries)
    sample_df = filtered_df.orderBy(rand()).limit(10).select(
        col("message").alias("log_entry"), col("log_level")
    )

    # Output directories
    os.makedirs("data/output", exist_ok=True)
    tmp_counts = "data/output/tmp_counts"
    tmp_sample = "data/output/tmp_sample"

    # Write to temporary directories (worker writes)
    counts_df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_counts)
    sample_df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_sample)

    counts_path = os.path.expanduser("~/spark-cluster/problem1_counts.csv")
    sample_path = os.path.expanduser("~/spark-cluster/problem1_sample.csv")
    counts_df.toPandas().to_csv(counts_path, index=False)
    sample_df.toPandas().to_csv(sample_path, index=False)

    # Keep existing move logic (just in case)
    move_single_csv(tmp_counts, "data/output/problem1_counts.csv")
    move_single_csv(tmp_sample, "data/output/problem1_sample.csv")

    # Generate summary text (also saved under ~/spark-cluster/)
    total_lines = logs_df.count()
    valid_lines = filtered_df.count()
    unique_levels = filtered_df.select("log_level").distinct().count()
    counts = counts_df.collect()

    summary_text = []
    summary_text.append(f"Total log lines processed: {total_lines}")
    summary_text.append(f"Total lines with log levels: {valid_lines}")
    summary_text.append(f"Unique log levels found: {unique_levels}\n")
    summary_text.append("Log level distribution:")
    for row in counts:
        pct = (row['count'] / valid_lines) * 100 if valid_lines > 0 else 0
        summary_text.append(f"  {row['log_level']:<6}: {row['count']:>10,} ({pct:6.2f}%)")

    summary_path = os.path.expanduser("~/spark-cluster/problem1_summary.txt")
    with open(summary_path, "w") as f:
        f.write("\n".join(summary_text))

    print("\n".join(summary_text))
    spark.stop()

if __name__ == "__main__":
    main()
