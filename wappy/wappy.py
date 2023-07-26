"""Main module."""
from abc import ABC, abstractmethod
from typing import List

from great_expectations.dataset import SparkDFDataset
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable


class DataPipeline(ABC):
    def __init__(
        self, spark: SparkSession, source_df: DataFrame, staging_name: str, publish_name: str, merge_keys=List[str]
    ) -> None:
        self.spark = spark
        self.source_df = source_df
        self.staging_name = staging_name
        self.publish_name = publish_name
        self.merge_keys = merge_keys

    def write(self) -> None:
        self.source_df.write.mode("overwrite").format("delta").saveAsTable(self.staging_name)
        self.staging_df = self.spark.table(self.staging_name)

    @abstractmethod
    def audit(self):
        if self.staging_df.count() == 0:
            raise ValueError("Dataframe is empty")
        if len(self.staging_df.columns) == 0:
            raise ValueError("No columns in the dataframe")

        # Convert PySpark DataFrame to Great Expectations Dataset
        df_ge = SparkDFDataset(self.staging_df)

        # Completeness check
        col_complete_failed = []
        # TODO: make columns a param
        for column in df.columns:
            if not df_ge.expect_column_values_to_not_be_null(column).success:
                col_complete_failed.append(column)
                raise ValueError(f"Column {column} has null values")

        # Consistency check
        if 'date' in df.columns:
            if not df_ge.expect_column_values_to_be_of_type('date', 'datetime64[ns]').success:
                raise ValueError("Inconsistent date format")

        if 'percentage' in df.columns:
            if not df_ge.expect_column_values_to_be_between('percentage', 0, 100).success:
                raise ValueError("Inconsistent percentage values. All values should be between 0 and 100.")

        # Uniqueness check
        if not df_ge.expect_table_row_count_to_be_between(min_value=1, max_value=df_ge.shape[0]).success:
            raise ValueError("Dataframe contains duplicate rows")

        # Timeliness check
        if 'date' in df.columns:
            thirty_days_ago = datetime.now() - timedelta(days=30)
            if not df_ge.expect_column_values_to_be_between('date', thirty_days_ago, datetime.now()).success:
                raise ValueError("Some dates are older than 30 days")

        # Relevance check
        required_columns = ['id', 'date', 'percentage']  # add all required column names here
        for column in required_columns:
            if not df_ge.expect_column_to_exist(column).success:
                raise ValueError(f"Missing required column: {column}")

        # Accuracy & Validity check
        if 'percentage' in df.columns:
            if not df_ge.expect_column_values_to_be_between('percentage', 0, 100).success:
                raise ValueError("Percentage values are over 100")

        if 'id' in df.columns:
            if not df_ge.expect_column_values_to_be_between('id', 0, None).success:
                raise ValueError("ID values are negative")
        pass

    def publish(self):
        table_exists = self.spark.catalog.tableExists(self.publish_name)
        if table_exists:
            old_alias = "target"
            new_alias = "source"
            merge_cond = " AND ".join([f"{old_alias}.{key} = {new_alias}.{key}" for key in self.merge_keys])
            delta_table = DeltaTable.forName(self.spark, self.publish_name)
            delta_table.alias(old_alias).merge(
                self.staging_df.alias(new_alias),
                merge_cond,
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            self.staging_df.write.format("delta").saveAsTable(self.publish_name)

    def run(self):
        self.write()
        self.audit()
        self.publish()


class DeltaDataPipeline(DataPipeline):
    def audit(self):
        # ... your auditing logic here ...
        pass
