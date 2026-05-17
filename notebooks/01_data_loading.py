from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    input_file_name,
    regexp_extract,
    to_date,
    format_string,
    substring,
    col,
    coalesce,
    array,
    row_number,
)
from pyspark.sql.window import Window


RAW_PATH = "/data/ProjectDatasetFacebookAU/"
PARQUET_PATH = "/user/s3348393/main/preprocessing/v1/parquet"
WINDOW_START = "2021-11-21"
WINDOW_END = "2022-11-21"


def load_raw(spark: SparkSession, path: str) -> DataFrame:
    """
    load the data from the raw jsons. include the filename in a column
    snapshot_date. the date format changes a bit so the regex is designed
    to handle that. 
    """
    df = spark.read.json(path)
    df = df.withColumn("filename", input_file_name())

    date_re = r"-(\d{4})(\d{1,2})(\d{2})-"
    yyyy = regexp_extract("filename", date_re, 1).cast("int")
    mm = regexp_extract("filename", date_re, 2).cast("int")
    dd = regexp_extract("filename", date_re, 3).cast("int")
    date_str = format_string("%d-%02d-%02d", yyyy, mm, dd)

    df = df.withColumn("snapshot_date", to_date(date_str, "yyyy-MM-dd"))
    return df


def cast_dates(df: DataFrame) -> DataFrame:
    """
    cast datetime strings to date types. 
    """
    for src in ["ad_creation_time", "ad_delivery_start_time", "ad_delivery_stop_time"]:
        dst = src.replace("_time", "_date")
        df = df.withColumn(dst, to_date(substring(src, 1, 10), "yyyy-MM-dd"))
    return df


def collapse_schema(df: DataFrame) -> DataFrame:
    """
    schema cleanup, based upon the analysis in notebook 01
    """
    df = df.select(
        "id",
        "page_id",
        "page_name",
        "snapshot_date",
        "ad_creation_date",
        "ad_delivery_start_date",
        "ad_delivery_stop_date",
        coalesce(col("ad_creative_bodies"), array(col("ad_creative_body"))).alias("creative_bodies"),
        coalesce(col("ad_creative_link_captions"), array(col("ad_creative_link_caption"))).alias("creative_link_captions"),
        coalesce(col("ad_creative_link_descriptions"), array(col("ad_creative_link_description"))).alias("creative_link_descs"),
        coalesce(col("ad_creative_link_titles"), array(col("ad_creative_link_title"))).alias("creative_link_titles"),
        "spend_lower_bound",
        "spend_upper_bound",
        "spend_mid",
        "impressions_lower_bound",
        "impressions_upper_bound",
        "impressions_mid",
        "audience_size_lower_bound",
        "audience_size_upper_bound",
        "audience_size_mid",
        "currency",
        "languages",
        "publisher_platforms",
        "demographic_distribution",
        coalesce(col("delivery_by_region"), col("region_distribution")).alias("delivery_by_region"),
        "ad_snapshot_url",
        coalesce(col("bylines"), col("funding_entity")).alias("bylines"),
        "ad_seq_no",
    )
    return df


def filter_election_window(df: DataFrame, start: str, end: str) -> DataFrame:
    """
    limit data to 6 months before/after the 2022 federal election
    """
    df = df.filter((col("ad_creation_date") >= start) & (col("ad_creation_date") <= end))
    return df


def flatten_numeric_structs(df: DataFrame) -> DataFrame:
    """
    we have a couple of structs like spend that have a min
    and max value as a string. this casts them to longs
    then finds the average value. drops the originals.
    """
    df = df.withColumn("spend_lower_bound", col("spend.lower_bound").cast("long"))
    df = df.withColumn("spend_upper_bound", col("spend.upper_bound").cast("long"))
    df = df.withColumn("spend_mid", (col("spend_lower_bound") + col("spend_upper_bound")) / 2.0)

    df = df.withColumn("impressions_lower_bound", col("impressions.lower_bound").cast("long"))
    df = df.withColumn("impressions_upper_bound", col("impressions.upper_bound").cast("long"))
    df = df.withColumn("impressions_mid", (col("impressions_lower_bound") + col("impressions_upper_bound")) / 2.0)

    df = df.withColumn("audience_size_lower_bound", col("estimated_audience_size.lower_bound").cast("long"))
    df = df.withColumn("audience_size_upper_bound", col("estimated_audience_size.upper_bound").cast("long"))
    df = df.withColumn("audience_size_mid", (col("audience_size_lower_bound") + col("audience_size_upper_bound")) / 2.0)

    df = df.drop("spend", "impressions", "estimated_audience_size")
    return df


def add_snapshot_sequence(df: DataFrame) -> DataFrame:
    """window partition into ad id, then order by snapshot date,
    latest first, then label them with row numbers. e.g. filter by ad_seq_no = 1
    to get the deduped dataset, but at this stage we dont wipe the other rows
    """
    w = Window.partitionBy("id").orderBy(col("snapshot_date").desc())
    return df.withColumn("ad_seq_no", row_number().over(w))


def write_output(df: DataFrame, path: str) -> None:
    """ write into parquet with 8 extents"""
    df.coalesce(8).write.parquet(path, mode="overwrite")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("FB_API_topics")
        .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter")
        .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )

    df = load_raw(spark, RAW_PATH)
    df = cast_dates(df)
    df = filter_election_window(df, WINDOW_START, WINDOW_END)
    df = flatten_numeric_structs(df)
    df = add_snapshot_sequence(df)
    df = collapse_schema(df)
    write_output(df, PARQUET_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
