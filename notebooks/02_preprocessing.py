import re

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, coalesce, lit, pandas_udf
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover


IN_PATH = "/user/s3348393/main/preprocessing/v1/parquet"
OUT_PATH = "/user/s3348393/main/preprocessing/v2/parquet"
HOUSE_CSV = "../data/2022_election_candidates.csv"
SENATE_CSV = "../data/2022_senate_candidates.csv"
PARTIES_CSV = "../data/aec_parties.csv"

HONORIFICS = ["mp", "hon", "dr", "mr", "mrs", "ms", "sen", "senator", "rt", "authorised", "by", "for"]

GOV_BYLINE_SIGNATURES = [
    frozenset({"department"}),
    frozenset({"australian", "government"}),
    frozenset({"australian", "electoral", "commission"}),
    frozenset({"australian", "cyber", "security"}),
]

COMMERCIAL_BYLINE_SIGNATURES = [
    frozenset({"shell"}),
]

INTERMEDIATE_COLUMNS = [
    "page_name_safe", "bylines_safe",
    "page_name_tokens", "bylines_tokens",
    "page_name_terms", "bylines_terms",
]


def load_v1(spark: SparkSession, path: str) -> DataFrame:
    """
    read the v1 parquet written by 01_data_loading.py.
    """
    return spark.read.parquet(path)


def load_candidates(house_csv: str, senate_csv: str) -> pd.DataFrame:
    """
    load 2022 house + senate candidates from the AEC csvs and stack them
    into a single frame. the senate csv has PartyAB rather than PartyAb
    (filled in by 01_5_senate_party_codes.ipynb) so we rename it before
    concatenating, then the downstream code can treat house and senate
    rows the same way.
    """
    house = pd.read_csv(house_csv, header=0)
    senate = pd.read_csv(senate_csv, header=0).rename(columns={"PartyAB": "PartyAb"})

    candidates = pd.concat(
        [house[["PartyAb", "PartyNm", "Surname", "GivenNm"]],
         senate[["PartyAb", "PartyNm", "Surname", "GivenNm"]]],
        ignore_index=True,
    )
    return candidates


def tokenise_text_columns(df: DataFrame) -> DataFrame:
    """
    tokenise page_name and bylines separately so a given name in page_name
    can't cross with a surname in bylines and spuriously match a candidate.
    coalesce nulls to empty strings first - RegexTokenizer blows up on
    nulls. then RegexTokenizer (splits on \\W+, lowercases) and
    StopWordsRemover (english defaults plus a few political honorifics).
    """
    stopwords = StopWordsRemover.loadDefaultStopWords("english") + HONORIFICS

    df = df.withColumn("page_name_safe", coalesce(col("page_name"), lit("")))
    df = RegexTokenizer(
        inputCol="page_name_safe", outputCol="page_name_tokens",
        pattern=r"\W+", toLowercase=True,
    ).transform(df)
    df = StopWordsRemover(
        inputCol="page_name_tokens", outputCol="page_name_terms",
        stopWords=stopwords,
    ).transform(df)

    df = df.withColumn("bylines_safe", coalesce(col("bylines"), lit("")))
    df = RegexTokenizer(
        inputCol="bylines_safe", outputCol="bylines_tokens",
        pattern=r"\W+", toLowercase=True,
    ).transform(df)
    df = StopWordsRemover(
        inputCol="bylines_tokens", outputCol="bylines_terms",
        stopWords=stopwords,
    ).transform(df)

    return df


def build_candidate_index(candidates: pd.DataFrame) -> list:
    """
    build the candidate match index. for each candidate the required
    tokens are the first given-name token plus all surname tokens - e.g.
    Adam ABDUL RAZAK becomes {adam, abdul, razak}. token-set subset
    matching is order-insensitive so "Adam Abdul Razak" and
    "Abdul Razak, Adam" both match. drops rows with missing surname or a
    non-string PartyAb.
    """
    def split_tokens(s):
        if not isinstance(s, str):
            return []
        return [t for t in re.split(r"\W+", s.lower()) if t]

    candidate_list = []
    for _, row in candidates.iterrows():
        surname_toks = split_tokens(row["Surname"])
        if not surname_toks:
            continue
        given_toks = split_tokens(row["GivenNm"])
        required = frozenset(surname_toks + given_toks[:1])
        party = row["PartyAb"]
        if not isinstance(party, str):
            continue
        candidate_list.append((required, party))

    return candidate_list


def load_party_byline_signatures(parties_csv: str) -> list:
    """
    load aec_parties.csv into a list of (frozenset_of_tokens, party_ab).
    skips rows where byline_tokens is empty. preserves CSV order so
    specific signatures like {liberal, national} get a chance to match
    before generic ones like {liberal}.
    """
    aec_parties = pd.read_csv(parties_csv, header=0)

    signatures = []
    for _, row in aec_parties.iterrows():
        bt = row["byline_tokens"]
        if not isinstance(bt, str) or not bt.strip():
            continue
        sig = frozenset(t.strip() for t in bt.split("+") if t.strip())
        signatures.append((sig, row["party_ab"]))

    return signatures


def classify_ads(df: DataFrame, candidate_list: list, party_byline_signatures: list) -> DataFrame:
    """
    apply the priority classifier as a pandas_udf and add political_party
    and match_type columns. priority is candidate then party_org then
    government then commercial. ambiguous candidate matches (more than one
    party agrees) fall through to the byline checks. pandas_udf uses Arrow
    to batch data between the JVM and python workers, which is meaningfully
    faster than a plain row-at-a-time udf.
    """
    result_schema = StructType([
        StructField("political_party", StringType(), True),
        StructField("match_type", StringType(), True),
    ])

    def _classify_one(page_tokens, bylines_tokens):
        # pandas_udf hands array columns to python as numpy arrays. an empty-or-None
        # check using `or []` would call bool() on the array, which is ambiguous for
        # multi-element arrays. so be explicit.
        page_set = set(page_tokens) if page_tokens is not None else set()
        bylines_set = set(bylines_tokens) if bylines_tokens is not None else set()

        # 1. candidate name match (house + senate)
        parties = set()
        for required, party in candidate_list:
            if required.issubset(page_set) or required.issubset(bylines_set):
                parties.add(party)
        if len(parties) == 1:
            return (next(iter(parties)), "candidate")

        # 2. party-name byline signature
        for sig, party in party_byline_signatures:
            if sig.issubset(bylines_set):
                return (party, "party_org")

        # 3. government byline signature
        for sig in GOV_BYLINE_SIGNATURES:
            if sig.issubset(bylines_set):
                return (None, "government")

        # 4. commercial byline signature
        for sig in COMMERCIAL_BYLINE_SIGNATURES:
            if sig.issubset(bylines_set):
                return (None, "commercial")

        return (None, None)

    @pandas_udf(result_schema)
    def classify_ad(page_terms: pd.Series, bylines_terms: pd.Series) -> pd.DataFrame:
        results = [_classify_one(p, b) for p, b in zip(page_terms, bylines_terms)]
        return pd.DataFrame(results, columns=["political_party", "match_type"])

    df = df.withColumn("_class", classify_ad("page_name_terms", "bylines_terms"))
    df = df.withColumn("political_party", col("_class.political_party"))
    df = df.withColumn("match_type", col("_class.match_type"))
    df = df.drop("_class")
    return df


def drop_intermediate_columns(df: DataFrame) -> DataFrame:
    """
    drop the safe/tokens/terms columns. they're big arrays and easy to
    rebuild from page_name and bylines later if anything downstream wants
    them back.
    """
    return df.drop(*INTERMEDIATE_COLUMNS)


def write_v2(df: DataFrame, path: str) -> None:
    """
    write parquet partitioned by political_party. rows with null
    political_party land in political_party=__HIVE_DEFAULT_PARTITION__/
    which is expected - that's the bulk of the corpus.
    """
    df.write.partitionBy("political_party").parquet(path, mode="overwrite")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("FB_API_party_match")
        .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter")
        .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )

    df = load_v1(spark, IN_PATH)

    candidates = load_candidates(HOUSE_CSV, SENATE_CSV)
    candidate_list = build_candidate_index(candidates)
    party_byline_signatures = load_party_byline_signatures(PARTIES_CSV)

    df = tokenise_text_columns(df)
    df = classify_ads(df, candidate_list, party_byline_signatures)
    df = drop_intermediate_columns(df)
    write_v2(df, OUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
