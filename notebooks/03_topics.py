import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    array_contains, col, coalesce, concat_ws, desc, expr, length, lit, row_number,
)
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import CountVectorizer, RegexTokenizer, StopWordsRemover
from pyspark.ml.functions import vector_to_array


V2_PATH = "/user/s3348393/main/preprocessing/v2/parquet"
INTERMEDIATE_PATH = "/user/s3348393/main/preprocessing/v3/intermediate_parquet"
TOPIC_TERMS_CSV = "../data/topic_terms.csv"

# bylines we've identified as clearly non-political (commercial, recruitment,
# streaming services). grown iteratively when LDA surfaces a noise topic.
COMMERCIAL_BYLINES = {
    "Access",
    "Streamotion Pty Ltd",
    "SBS Australia",
    "SBS Arabic24",
    "SBS Mandarin中文普通话",
    "The Squiz",
    "Hair Cooki",
    "Shell"
}

# english defaults plus URL fragments, contraction debris, and generic fillers.
# kept short - minDF and maxDF in CountVectorizer handle most frequency-based
# filtering for us.
DOMAIN_STOP_WORDS = [
    "https", "http", "www", "com", "org", "au", "co", "html",
    "re", "ve", "ll",
    "help", "time", "like", "need", "make", "take", "people",
    "year", "years", "today", "also", "will", "can", "get",
    "see", "know", "one", "two", "new", "now", "us",
    "click", "learn",'australia', 'australian', "2022"
]

# LDA + CountVectorizer hyperparameters - picked from the k-sweep in 03_topics.ipynb.
K = 25
VOCAB_SIZE = 5000
MIN_DF = 100
MAX_DF = 0.3
LDA_MAX_ITER = 20
SEED = 42
TOP_TERMS_N = 15
TOP_BYLINES_N = 5


def load_v2(spark: SparkSession, path: str) -> DataFrame:
    """
    read the v2 parquet
    """
    return spark.read.parquet(path)


def first_non_empty(col_name: str):
    """
    spark way to get the first non-null value of an array
    """
    return expr(f"filter({col_name}, x -> x is not null and length(x) > 0)[0]")


def filter_to_residual_subset(df: DataFrame, commercial_bylines: set) -> DataFrame:
    """
    get the non-goverment, non-comercial, english language rows
    """
    return df.filter(
        (col("ad_seq_no") == 1)
        & col("match_type").isNull()
        & (col("languages").isNull() | array_contains("languages", "en"))
        & ~col("bylines").isin(list(commercial_bylines))
    )


def extract_body_text(df: DataFrame) -> DataFrame:
    """
    combine title, description and body, for those cases where the body is an image etc
    """
    df = df.withColumn("body_text", first_non_empty("creative_bodies"))
    df = df.withColumn("desc_text", first_non_empty("creative_link_descs"))
    df = df.withColumn("title_text", first_non_empty("creative_link_titles"))
    df = df.withColumn(
        "body",
        concat_ws(
            " ",
            coalesce(col("body_text"), lit("")),
            coalesce(col("desc_text"), lit("")),
            coalesce(col("title_text"), lit("")),
        ),
    )
    df = df.filter(length(col("body")) > 0)
    df = df.drop("body_text", "desc_text", "title_text")
    return df


def build_preprocessing_pipeline(stop_words: list) -> Pipeline:
    """
    three stage pipeline. 
    """
    tokenizer = RegexTokenizer(
        inputCol="body", outputCol="raw_tokens",
        pattern=r"\W+", toLowercase=True, minTokenLength=2,
    )

    remover = StopWordsRemover(
        inputCol="raw_tokens", outputCol="tokens",
        stopWords=stop_words,
    )

    vectorizer = CountVectorizer(
        inputCol="tokens", outputCol="features",
        vocabSize=VOCAB_SIZE,
        minDF=MIN_DF,
        maxDF=MAX_DF,
    )

    return Pipeline(stages=[tokenizer, remover, vectorizer])


def fit_preprocessing(corpus: DataFrame, pipeline: Pipeline):
    """
    run prepro and cache
    """
    prep_model = pipeline.fit(corpus)
    features_df = prep_model.transform(corpus).cache()
    return prep_model, features_df


def fit_lda(features_df: DataFrame, k: int) -> "LDAModel":
    """
    fit LDA using 20 topics
    """
    lda = LDA(featuresCol="features", k=k, maxIter=LDA_MAX_ITER, seed=SEED)
    return lda.fit(features_df)


def attach_topic_ids(lda_model, features_df: DataFrame) -> DataFrame:
    """
    attach the LDA results back to the df
    """
    return (
        lda_model.transform(features_df)
        .withColumn("topic_array", vector_to_array("topicDistribution"))
        .withColumn("topic_id", expr("array_position(topic_array, array_max(topic_array)) - 1"))
    )


def compute_top_terms(lda_model, vocab: list) -> dict:
    """
    get the top terms for each topic
    """
    rows = lda_model.describeTopics(maxTermsPerTopic=TOP_TERMS_N).collect()
    return {row.topic: [vocab[i] for i in row.termIndices] for row in rows}


def compute_top_bylines(classified: DataFrame) -> dict:
    """
    get the top bylines for each topic
    """
    w = Window.partitionBy("topic_id").orderBy(desc("count"))
    rows = (
        classified.filter(col("bylines").isNotNull())
        .groupBy("topic_id", "bylines").count()
        .withColumn("rank", row_number().over(w))
        .filter(col("rank") <= TOP_BYLINES_N)
        .orderBy("topic_id", "rank")
        .collect()
    )

    top_bylines = {}
    for row in rows:
        top_bylines.setdefault(row.topic_id, []).append(row.bylines)
    return top_bylines


def write_topic_terms_csv(top_terms: dict, top_bylines: dict, path: str) -> None:
    """
    write out out to a csv so we can manually tag the topics
    """
    rows = []
    for tid in sorted(top_terms.keys()):
        rows.append({
            "topic_id":    tid,
            "top_terms":   " ".join(top_terms.get(tid, [])),
            "top_bylines": "|".join(top_bylines.get(tid, [])),
            "label":       "",
        })
    pd.DataFrame(rows).to_csv(path, index=False)


def drop_lda_intermediates(df: DataFrame) -> DataFrame:
    """
    clean up
    """
    return df.drop("raw_tokens", "tokens", "features", "topic_array")


def write_intermediate(df: DataFrame, path: str) -> None:
    """
    write out the corpus with the topic labels etc so we dont need to re-run
    """
    df.write.parquet(path, mode="overwrite")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("FB_API_topics")
        .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter")
        .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )

    df = load_v2(spark, V2_PATH)
    corpus = filter_to_residual_subset(df, COMMERCIAL_BYLINES)
    corpus = extract_body_text(corpus)

    stop_words = StopWordsRemover.loadDefaultStopWords("english") + DOMAIN_STOP_WORDS
    pipeline = build_preprocessing_pipeline(stop_words)
    prep_model, features_df = fit_preprocessing(corpus, pipeline)

    lda_model = fit_lda(features_df, K)
    classified = attach_topic_ids(lda_model, features_df)

    vocab = prep_model.stages[-1].vocabulary
    top_terms = compute_top_terms(lda_model, vocab)
    top_bylines = compute_top_bylines(classified)

    write_topic_terms_csv(top_terms, top_bylines, TOPIC_TERMS_CSV)
    classified = drop_lda_intermediates(classified)
    write_intermediate(classified, INTERMEDIATE_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
