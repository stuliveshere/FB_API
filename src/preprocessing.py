"""
Build the canonical 2022 federal election corpus from the FB Ad Library dataset.

Loads raw JSON, normalises schema drift (singular/plural pairs, deprecated field
renames), casts dates, flattens numeric structs, filters to the 6-month window
preceding the 21 May 2022 federal election, and writes a versioned parquet.

Usage:
    From a notebook:
        from src.preprocessing import build_corpus
        df = build_corpus(spark)

    Standalone:
        spark-submit src/preprocessing.py
"""

import json
import os
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    coalesce,
    col,
    count as spark_count,
    countDistinct,
    format_string,
    input_file_name,
    max as spark_max,
    min as spark_min,
    regexp_extract,
    row_number,
    substring,
    to_date,
)
from pyspark.sql.window import Window


# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

VERSION = "v1"

DATA_PATH    = "/data/ProjectDatasetFacebookAU/"
PARQUET_PATH = f"/user/s3348393/main/preprocessing/{VERSION}/parquet"

LOG_DIR      = f"/home/s3348393/FB_API/logs/main/preprocessing/{VERSION}"
LOG_PATH     = f"{LOG_DIR}/build.log"
LOG_JSON     = f"{LOG_DIR}/build.json"

ELECTION_DATE = "2022-05-21"
WINDOW_START  = "2021-11-21"  # 6 months prior to election

# parquet output partitioning — small corpus, ~8 files of ~50MB
OUTPUT_PARTITIONS = 8


# ----------------------------------------------------------------------------
# Pipeline steps
# ----------------------------------------------------------------------------

def load_raw(spark):
    """Read all JSON files from HDFS as-is."""
    return spark.read.json(DATA_PATH)


def add_snapshot_date(df):
    """Parse snapshot date from filename. Handles both 8-digit and 7-digit
    date formats (some files have a single-digit month without zero-padding)."""
    date_re = r"-(\d{4})(\d{1,2})(\d{2})-"

    yyyy = regexp_extract("filename", date_re, 1).cast("int")
    mm   = regexp_extract("filename", date_re, 2).cast("int")
    dd   = regexp_extract("filename", date_re, 3).cast("int")

    date_str = format_string("%d-%02d-%02d", yyyy, mm, dd)

    return df \
        .withColumn("filename", input_file_name()) \
        .withColumn("snapshot_date", to_date(date_str, "yyyy-MM-dd"))


def cast_dates(df):
    """Cast string date columns to typed date columns. Handles both the short
    'yyyy-MM-dd' format (most rows) and the long 'yyyy-MM-ddTHH:mm:ss+0000'
    format (early-period rows) by slicing the first 10 characters."""
    for src in ["ad_creation_time", "ad_delivery_start_time", "ad_delivery_stop_time"]:
        dst = src.replace("_time", "_date")
        df = df.withColumn(dst, to_date(substring(src, 1, 10), "yyyy-MM-dd"))
    return df


def filter_window(df):
    """Restrict to the 6 months preceding the 2022 federal election."""
    return df.filter(
        (col("ad_creation_date") >= WINDOW_START) &
        (col("ad_creation_date") <= ELECTION_DATE)
    )


def normalise_schema(df):
    """Coalesce deprecated/renamed field pairs and drop original split columns.
    See docs/release_notes.md for the deprecation timeline."""
    df.createOrReplaceTempView("ads")
    return df.sparkSession.sql("""
        SELECT
            id,
            page_id,
            page_name,
            snapshot_date,
            ad_creation_date,
            ad_delivery_start_date,
            ad_delivery_stop_date,
            coalesce(ad_creative_bodies,            array(ad_creative_body))            AS creative_bodies,
            coalesce(ad_creative_link_captions,     array(ad_creative_link_caption))    AS creative_link_captions,
            coalesce(ad_creative_link_descriptions, array(ad_creative_link_description)) AS creative_link_descs,
            coalesce(ad_creative_link_titles,       array(ad_creative_link_title))      AS creative_link_titles,
            impressions,
            spend,
            estimated_audience_size,
            currency,
            languages,
            publisher_platforms,
            demographic_distribution,
            coalesce(delivery_by_region, region_distribution) AS delivery_by_region,
            ad_snapshot_url,
            coalesce(bylines, funding_entity) AS bylines
        FROM ads
    """)


def flatten_structs(df):
    """Flatten the three numeric structs (spend, impressions, estimated_audience_size)
    into typed long columns plus a midpoint. Drops the original struct columns."""
    for parent, prefix in [
        ("spend",                   "spend"),
        ("impressions",             "impressions"),
        ("estimated_audience_size", "audience_size"),
    ]:
        lo = col(f"{parent}.lower_bound").cast("long")
        hi = col(f"{parent}.upper_bound").cast("long")
        df = df.withColumn(f"{prefix}_lower_bound", lo)
        df = df.withColumn(f"{prefix}_upper_bound", hi)
        df = df.withColumn(f"{prefix}_mid", (lo + hi) / 2.0)

    return df.drop("spend", "impressions", "estimated_audience_size")


def add_snapshot_seq(df):
    """Add ad_seq_no: 1 = most recent snapshot per ad, ascending into the past."""
    w = Window.partitionBy("id").orderBy(col("snapshot_date").desc())
    return df.withColumn("ad_seq_no", row_number().over(w))


def build_corpus(spark):
    """Run the full preprocessing pipeline and return the cleaned DataFrame.

    Order matters: cast dates and filter the window early so subsequent
    operations work on the smaller windowed dataset (~5M rows vs ~40M).
    """
    df = load_raw(spark)
    df = add_snapshot_date(df)
    df = cast_dates(df)
    df = filter_window(df)
    df = normalise_schema(df)
    df = flatten_structs(df)
    df = add_snapshot_seq(df)
    return df


# ----------------------------------------------------------------------------
# Build logging
# ----------------------------------------------------------------------------

def collect_stats(spark, raw_df, final_df):
    """Gather build statistics. Triggers a few Spark jobs."""
    stats = {}
    stats["timestamp"]    = datetime.now().isoformat(timespec="seconds")
    stats["version"]      = VERSION
    stats["source_path"]  = DATA_PATH
    stats["output_path"]  = PARQUET_PATH
    stats["window_start"] = WINDOW_START
    stats["window_end"]   = ELECTION_DATE

    # raw shape
    stats["raw_rows"]     = raw_df.count()
    stats["raw_columns"]  = len(raw_df.columns)

    # final shape
    stats["final_rows"]   = final_df.count()
    stats["final_columns"] = len(final_df.columns)
    stats["unique_ads"]   = final_df.select("id").distinct().count()

    # date range
    date_range = final_df.agg(
        spark_min("ad_creation_date").alias("min"),
        spark_max("ad_creation_date").alias("max"),
        spark_min("snapshot_date").alias("snap_min"),
        spark_max("snapshot_date").alias("snap_max"),
    ).first()
    stats["earliest_creation"] = str(date_range.min)
    stats["latest_creation"]   = str(date_range.max)
    stats["earliest_snapshot"] = str(date_range.snap_min)
    stats["latest_snapshot"]   = str(date_range.snap_max)

    # field population fractions for key analytical columns
    n = stats["final_rows"]
    pop = {}
    for c in [
        "bylines", "page_name", "creative_bodies", "creative_link_captions",
        "spend_mid", "impressions_mid", "audience_size_mid",
        "delivery_by_region", "demographic_distribution", "languages",
        "snapshot_date",
    ]:
        not_null = final_df.filter(col(c).isNotNull()).count()
        pop[c] = {"populated": not_null, "pct": round(100 * not_null / n, 1)}
    stats["population"] = pop

    # top 20 funders — sanity check that political content is present
    top_bylines = final_df.filter(col("ad_seq_no") == 1) \
        .groupBy("bylines") \
        .agg(spark_count("*").alias("count")) \
        .orderBy(col("count").desc()) \
        .limit(20) \
        .collect()
    stats["top_bylines"] = [(r.bylines, r["count"]) for r in top_bylines]

    return stats


def render_log(stats, raw_df, final_df):
    """Render the stats dict as a human-readable log."""
    lines = [
        "=" * 70,
        f"FB_API preprocessing build — {stats['version']}",
        "=" * 70,
        f"Built:      {stats['timestamp']}",
        f"Source:     {stats['source_path']}",
        f"Output:     {stats['output_path']}",
        f"Window:     {stats['window_start']} → {stats['window_end']}",
        "",
        "--- BEFORE ---",
        f"Raw rows:    {stats['raw_rows']:>15,}",
        f"Raw columns: {stats['raw_columns']:>15}",
        "",
        "Raw schema:",
        raw_df._jdf.schema().treeString(),
        "",
        "--- AFTER ---",
        f"Final rows:    {stats['final_rows']:>15,}",
        f"Final columns: {stats['final_columns']:>15}",
        f"Unique ads:    {stats['unique_ads']:>15,}",
        "",
        f"Creation date range:  {stats['earliest_creation']} → {stats['latest_creation']}",
        f"Snapshot date range:  {stats['earliest_snapshot']} → {stats['latest_snapshot']}",
        "",
        "Final schema:",
        final_df._jdf.schema().treeString(),
        "",
        "--- FIELD POPULATION ---",
    ]
    for c, p in stats["population"].items():
        lines.append(f"  {c:30s} {p['populated']:>10,}  ({p['pct']:>5}%)")

    lines.extend([
        "",
        "--- TOP 20 FUNDERS (by ad count, latest snapshot only) ---",
    ])
    for byline, n in stats["top_bylines"]:
        label = byline if byline is not None else "<NULL>"
        lines.append(f"  {label[:50]:50s} {n:>10,}")

    lines.extend([
        "",
        f"Build duration: {stats.get('duration_seconds', 'unknown')}s",
        "=" * 70,
    ])
    return "\n".join(lines)


def write_log(stats, raw_df, final_df):
    """Write log to local filesystem as both human-readable text and JSON."""
    os.makedirs(LOG_DIR, exist_ok=True)

    text = render_log(stats, raw_df, final_df)
    with open(LOG_PATH, "w") as f:
        f.write(text)

    with open(LOG_JSON, "w") as f:
        json.dump(stats, f, indent=2)

    print(f"Log: {LOG_PATH}")


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------

def main():
    start = time.time()

    spark = SparkSession.builder \
        .appName("FB_API_preprocessing") \
        .config("spark.sql.parquet.output.committer.class",
                "org.apache.parquet.hadoop.ParquetOutputCommitter") \
        .config("mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .getOrCreate()

    print(f"Loading raw data from {DATA_PATH}")
    raw_df = load_raw(spark)
    raw_df = add_snapshot_date(raw_df)  # reuse for stats (filename + snapshot_date)

    print("Building corpus")
    final_df = build_corpus(spark)
    final_df.cache()  # we'll scan it several times for stats + write

    print("Collecting build statistics")
    stats = collect_stats(spark, raw_df, final_df)
    stats["duration_seconds"] = round(time.time() - start, 1)

    print(f"Writing parquet to {PARQUET_PATH}")
    final_df.coalesce(OUTPUT_PARTITIONS).write.parquet(PARQUET_PATH, mode="overwrite")

    print("Writing build log")
    write_log(stats, raw_df, final_df)

    print(f"\nDone in {stats['duration_seconds']}s")
    print(f"  Rows written: {stats['final_rows']:,}")
    print(f"  Unique ads:   {stats['unique_ads']:,}")
    print(f"  Parquet:      {PARQUET_PATH}")
    print(f"  Log:          {LOG_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()