import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, min as spark_min, max as spark_max,
    input_file_name, try_to_timestamp
)


def main():
    # --- Parse arguments ---
    if len(sys.argv) < 3:
        print("Usage: python problem2.py <spark-master-url> --net-id <YOUR_NET_ID>")
        sys.exit(1)

    master_url = sys.argv[1]
    net_id = sys.argv[3] if len(sys.argv) >= 4 else "unknown"

    # --- Spark Session ---
    spark = (
        SparkSession.builder
        .appName("Problem2_ClusterUsageAnalysis")
        .master(master_url)
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Prepare S3 path ---
    bucket = os.environ.get("SPARK_LOGS_BUCKET")
    if not bucket:
        raise EnvironmentError("SPARK_LOGS_BUCKET not found!")
    bucket = bucket.replace("s3://", "s3a://")
    s3_path = f"s3://{net_id}-assignment-spark-cluster-logs/data/application_*/*"
    print(f"Reading logs from: {s3_path}")

    # --- Read data ---
    df_raw = spark.read.option("recursiveFileLookup", "true").text(s3_path)
    df_raw = df_raw.withColumn("file_path", input_file_name())

    # --- Extract application & cluster IDs ---
    df = df_raw.withColumn(
        "application_id", regexp_extract(col("file_path"), r"(application_\d+_\d+)", 1)
    ).withColumn(
        "cluster_id", regexp_extract(col("file_path"), r"application_(\d+)_\d+", 1)
    )

    from pyspark.sql.functions import try_to_timestamp, lit

    # --- Extract timestamps from log text (use try_to_timestamp to skip invalid) ---
    df = df.withColumn(
        "timestamp_str",
        regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1)
    ).withColumn(
        "timestamp",
        try_to_timestamp(col("timestamp_str"), lit("yy/MM/dd HH:mm:ss"))
    ).filter(col("timestamp").isNotNull())


    # --- Compute app-level timeline ---
    timeline_df = (
        df.groupBy("cluster_id", "application_id")
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time")
        )
        .orderBy("cluster_id", "start_time")
    )

    # Save timeline CSV
    timeline_path = os.path.expanduser("~/spark-cluster/problem2_timeline.csv")
    timeline_df.toPandas().to_csv(timeline_path, index=False)
    print(f" Saved: {timeline_path}")

    # --- Cluster-level summary ---
    cluster_summary_df = (
        timeline_df.groupBy("cluster_id")
        .agg(
            spark_min("start_time").alias("cluster_first_app"),
            spark_max("end_time").alias("cluster_last_app"),
        )
        .join(
            timeline_df.groupBy("cluster_id").count().withColumnRenamed("count", "num_applications"),
            on="cluster_id",
            how="left"
        )
        .orderBy(col("num_applications").desc())
    )

    cluster_summary_path = os.path.expanduser("~/spark-cluster/problem2_cluster_summary.csv")
    cluster_summary_df.toPandas().to_csv(cluster_summary_path, index=False)
    print(f" Saved: {cluster_summary_path}")

    # --- Generate summary text ---
    total_clusters = cluster_summary_df.count()
    total_apps = timeline_df.count()
    avg_apps = total_apps / total_clusters if total_clusters > 0 else 0

    top_clusters = cluster_summary_df.orderBy(col("num_applications").desc()).limit(3).collect()
    lines = [
        f"Total unique clusters: {total_clusters}",
        f"Total applications: {total_apps}",
        f"Average applications per cluster: {avg_apps:.2f}\n",
        "Most heavily used clusters:",
    ]
    for row in top_clusters:
        lines.append(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")

    stats_path = os.path.expanduser("~/spark-cluster/problem2_stats.txt")
    with open(stats_path, "w") as f:
        f.write("\n".join(lines))
    print(f" Saved: {stats_path}")

    # --- Visualization ---
    pdf = cluster_summary_df.toPandas()
    tdf = timeline_df.toPandas()
    tdf["duration_sec"] = (tdf["end_time"] - tdf["start_time"]).dt.total_seconds()

    # Bar chart: number of applications per cluster
    plt.figure(figsize=(8, 5))
    sns.barplot(x="cluster_id", y="num_applications", data=pdf, palette="Spectral")
    plt.xticks(rotation=30)
    plt.title("Applications per Cluster")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of Applications")
    for index, row in pdf.iterrows():
        plt.text(index, row.num_applications + 1, str(row.num_applications), ha='center')
    bar_chart_path = os.path.expanduser("~/spark-cluster/problem2_bar_chart.png")
    plt.tight_layout()
    plt.savefig(bar_chart_path)
    plt.close()
    print(f" Saved: {bar_chart_path}")

    # Density plot: job duration distribution for largest cluster
    if not pdf.empty:
        largest_cluster = pdf.iloc[0]["cluster_id"]
        durations = tdf[tdf["cluster_id"] == largest_cluster]["duration_sec"]
        plt.figure(figsize=(8, 5))
        sns.histplot(durations, kde=True, log_scale=True)
        plt.title(f"Duration Distribution (Cluster {largest_cluster}, n={len(durations)})")
        plt.xlabel("Duration (seconds, log scale)")
        plt.ylabel("Frequency")
        density_plot_path = os.path.expanduser("~/spark-cluster/problem2_density_plot.png")
        plt.tight_layout()
        plt.savefig(density_plot_path)
        plt.close()
        print(f" Saved: {density_plot_path}")

    spark.stop()
    print(" Problem 2 completed successfully!")


if __name__ == "__main__":
    main()
