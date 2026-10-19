from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
)
from spark_job import process_transactions
import pytest

@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .appName("PySparkUnitTest")
        .master("local[*]")     # run Spark locally on my computer and use all available CPU cores
        .getOrCreate()
    )
    yield spark
    spark.stop()

class TestProcessTransactions:

    @pytest.fixture(autouse=True)
    def setup_data(self, spark):

        txn_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("cardholder_id", StringType(), False),
            StructField("merchant_id", StringType(), False),
            StructField("merchant_name", StringType(), False),
            StructField("merchant_category", StringType(), False),
            StructField("transaction_amount", DoubleType(), False),
            StructField("transaction_timestamp", StringType(), False),
            StructField("transaction_status", StringType(), False),
            StructField("fraud_flag", BooleanType(), False),
            StructField("merchant_location", StringType(), False),
        ])

        txn_data = [
            ("T001","CH001","M001","Walmart","Groceries",120.50,"2025-02-04T10:00:00Z","SUCCESS",False,"NY,USA"),
            ("T002","CH002","M002","Expedia","Travel",9500.75,"2025-02-04T12:30:00Z","PENDING",True,"TO,CA"),
            ("T003","CH003","M003","Amazon","Shopping",75.20,"2025-02-04T15:45:00Z","FAILED",False,"SF,USA")
        ]

        self.txn_df = spark.createDataFrame(txn_data, txn_schema)

        cards_schema = StructType([
            StructField("cardholder_id", StringType(), False),
            StructField("customer_name", StringType(), False),
            StructField("reward_points", IntegerType(), False),
            StructField("risk_score", DoubleType(), False)
        ])

        cards_data = [
            ("CH001","John Doe",4500,0.15),
            ("CH002","Jane Smith",1200,0.35),
            ("CH003","Ali Khan",8000,0.10)
        ]

        self.cards_df = spark.createDataFrame(cards_data, cards_schema)


    def test_categories(self):

        df = process_transactions(self.cards_df, self.txn_df)
        mapping = {
            row["transaction_id"]: row["transaction_category"]
            for row in df.select(
                "transaction_id", "transaction_category"
            ).collect()
        }
        assert mapping["T001"] == "Medium"
        assert mapping["T002"] == "High"
        assert mapping["T003"] == "Low"


    def test_risk_and_points(self):

        df = process_transactions(self.cards_df, self.txn_df)
        rows = {
            row["transaction_id"]: row.asDict()
            for row in df.collect()
        }
        assert rows['T002']['high_risk'] is True
        assert rows['T002']['fraud_risk_level'] == 'Critical'
        assert rows['T001']['updated_reward_points'] == 4512
        assert rows['T003']['updated_reward_points'] == 8008


    def test_processed_row_cnt(self):

        df = process_transactions(self.cards_df, self.txn_df)
        count = df.count()
        assert count == 3