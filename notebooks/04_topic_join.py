import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import broadcast, col


V2_PATH = "/user/s3348393/main/preprocessing/v2/parquet"
INTERMEDIATE_PATH = "/user/s3348393/main/preprocessing/v3/intermediate_parquet"
V3_PATH = "/user/s3348393/main/preprocessing/v3/parquet"
TOPIC_LABELS_CSV = "../data/topic_labels.csv"


def load_v2_unique(spark: SparkSession, path: str) -> DataFrame:
    """
    load v2 parquet and keep one row per ad.
    """
    return spark.read.parquet(path).filter(col("ad_seq_no") == 1)


def load_intermediate(spark: SparkSession, path: str) -> DataFrame:
    """
    load nb 03's intermediate parquet, keep just the columns needed for the
    join + downstream analysis. id is the join key.
    """
    return spark.read.parquet(path).select("id", "topic_id", "topicDistribution", "body")


def load_topic_labels(spark: SparkSession, path: str) -> DataFrame:
    """
    load the hand-edited topic labels csv. csv is on the edge node, not
    HDFS, so we go via pandas first to avoid spark trying to read it from
    HDFS. rename label to topic_label so it doesnt collide with anything.
    """
    labels_pdf = pd.read_csv(path)[["topic_id", "label", "category"]]
    return spark.createDataFrame(labels_pdf).withColumnRenamed("label", "topic_label")


def join_topic_labels(v2: DataFrame, intermediate: DataFrame, labels: DataFrame) -> DataFrame:
    """
    three way left join. v2 + LDA columns + topic labels. every v2 ad gets
    the LDA columns if it was in the residual corpus, otherwise null. the
    labels table is tiny (25 rows) so we broadcast it.
    """
    return (
        v2.join(intermediate, "id", "left")
          .join(broadcast(labels), "topic_id", "left")
    )


def filter_political_corpus(df: DataFrame) -> DataFrame:
    """
    keep registered party advertising (candidate, party_org) plus residual
    ads that came out of the LDA labelling with a category. drops
    government (out of scope), byline-tagged commercial (Shell etc.), and
    residual ads that didnt make it into the LDA corpus.
    """
    return df.filter(
        col("match_type").isin("candidate", "party_org")
        | (col("match_type").isNull() & col("category").isNotNull())
    )


def drop_intermediates(df: DataFrame) -> DataFrame:
    """
    drop body and topicDistribution before parquet write. body is big text
    and we have the structured columns we need; topicDistribution is the
    length-k probability vector and topic_id (argmax) is what downstream
    uses.
    """
    return df.drop("body", "topicDistribution")


def write_v3(df: DataFrame, path: str) -> None:
    """
    write v3 partitioned by category so nb 05 can read subsets selectively.
    """
    df.write.partitionBy("category").parquet(path, mode="overwrite")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("FB_API_v3_build")
        .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter")
        .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )

    v2 = load_v2_unique(spark, V2_PATH)
    intermediate = load_intermediate(spark, INTERMEDIATE_PATH)
    labels = load_topic_labels(spark, TOPIC_LABELS_CSV)

    classified = join_topic_labels(v2, intermediate, labels)
    v3 = filter_political_corpus(classified)
    v3 = drop_intermediates(v3)

    write_v3(v3, V3_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
