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
    into a single frame. i made a typo in the senate csv, so i'll fix it here
    instead of re-doing the csv.
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
    ok we're going to match the page name and byline to the candidates. 
    tokenising byline and page name
    """
    stopwords = StopWordsRemover.loadDefaultStopWords("english") + HONORIFICS

    #tokenize page names
    df = df.withColumn("page_name_safe", coalesce(col("page_name"), lit(""))) #cast nulls to empty strings
    df = RegexTokenizer(inputCol="page_name_safe", outputCol="page_name_tokens", pattern=r"\W+", toLowercase=True).transform(df)
    df = StopWordsRemover(inputCol="page_name_tokens", outputCol="page_name_terms",stopWords=stopwords).transform(df)

    #tokenize bylines
    df = df.withColumn("bylines_safe", coalesce(col("bylines"), lit("")))
    df = RegexTokenizer(inputCol="bylines_safe", outputCol="bylines_tokens",pattern=r"\W+", toLowercase=True).transform(df)
    df = StopWordsRemover(inputCol="bylines_tokens", outputCol="bylines_terms",stopWords=stopwords).transform(df)

    return df


def build_candidate_index(candidates: pd.DataFrame) -> list:
    """
    build the candidate match index. 
    """
    def split_tokens(s):
        """
        tokenise an input string like the regextokeniser, on whitespace
        """
        if not isinstance(s, str):
            return []
        return [t for t in re.split(r"\W+", s.lower()) if t]

    candidate_list = []
    for _, row in candidates.iterrows():
        surname_tokens = split_tokens(row["Surname"])
        given_tokens = split_tokens(row["GivenNm"])
        candidate_tokens = frozenset(surname_tokens + given_tokens[:1])
        party = row["PartyAb"]
        if not surname_tokens:
            continue
        if not isinstance(party, str):
            continue
        candidate_list.append((candidate_tokens, party))

    return candidate_list


def load_party_byline_signatures(parties_csv: str) -> list:
    """
    load aec_parties.csv into a list of (frozenset_of_tokens, party_ab).
    preserves CSV order so specific signatures like {liberal, national} get a chance to match
    before generic ones like {liberal}.
    """
    aec_parties = pd.read_csv(parties_csv, header=0)

    signatures = []
    for _, row in aec_parties.iterrows():
        byline_tokens = row["byline_tokens"]
        if not isinstance(byline_tokens, str) or not byline_tokens.strip():
            continue
        signature = frozenset(t.strip() for t in byline_tokens.split("+") if t.strip())
        signatures.append((signature, row["party_ab"]))

    return signatures


def classify_ads(df: DataFrame, candidate_list: list, party_byline_signatures: list) -> DataFrame:
    """
    apply the priority classifier as a pandas_udf and add political_party
    and match_type columns. priority is candidate then party_org then
    government then commercial. replaced original python udf with pandas udf...
    it was too slow.
    """
    #output schema for the udf
    result_schema = StructType([
        StructField("political_party", StringType(), True),
        StructField("match_type", StringType(), True),
    ])

    def _classify_row(page_tokens, bylines_tokens):
        """ inner function to identify/label gov. advertising.
        
        pandas_udf. row by row. 
        tries:
            candidate match against byline and page name
            party name against byline
            generic government keywords against byline
            a special case for shell
        if no matches, it returns all nones.
        """

        page_set = set(page_tokens) if page_tokens is not None else set()
        bylines_set = set(bylines_tokens) if bylines_tokens is not None else set()

        # candidate name match - page name or byline
        parties = set()
        for candidate_tokens, party in candidate_list:
            if candidate_tokens.issubset(page_set) or candidate_tokens.issubset(bylines_set):
                parties.add(party)
        if len(parties) == 1:
            return (next(iter(parties)), "candidate")

        #party name match - byline only
        for sig, party in party_byline_signatures:
            if sig.issubset(bylines_set):
                return (party, "party_org")

        #gov. generic name match - byline only
        for sig in GOV_BYLINE_SIGNATURES:
            if sig.issubset(bylines_set):
                return (None, "government")

        #this is a special case for shell, which had a single, large non
        #political compaign during the election window
        for sig in COMMERCIAL_BYLINE_SIGNATURES:
            if sig.issubset(bylines_set):
                return (None, "commercial")

        return (None, None)

    #run the udf
    @pandas_udf(result_schema)
    def classify_ad(page_terms: pd.Series, bylines_terms: pd.Series) -> pd.DataFrame:
        results = [_classify_row(p, b) for p, b in zip(page_terms, bylines_terms)]
        return pd.DataFrame(results, columns=["political_party", "match_type"])

    #unpack the results
    df = df.withColumn("_class", classify_ad("page_name_terms", "bylines_terms"))
    df = df.withColumn("political_party", col("_class.political_party"))
    df = df.withColumn("match_type", col("_class.match_type"))
    df = df.drop("_class")
    return df


def drop_intermediate_columns(df: DataFrame) -> DataFrame:
    """
    cleanup
    """
    df = df.drop(*INTERMEDIATE_COLUMNS)
    return df


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
