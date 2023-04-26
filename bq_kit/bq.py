from enum import Enum, auto

from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.cloud.exceptions import NotFound
import pandas as pd
import pyarrow as pa

from .common import get_credentials


class DataFormat(Enum):
    pandas = auto()
    arrow = auto()


class BigQuery:
    def __init__(self, project_name: str, scopes: list | None = None) -> None:
        self.project_name = project_name
        self.scopes = scopes
        self.credentials = get_credentials(self.scopes)
        self.bq_client = bigquery.Client(
            project=self.project_name, credentials=self.credentials)
        self.bq_storage_client = None
        if self.credentials.has_scopes(["https://www.googleapis.com/auth/cloud-platform"]):
            self.bq_storage_client = bigquery_storage.BigQueryReadClient(
                credentials=self.credentials)

    def __bq_to(self, sql: str, data_format: DataFormat) -> pd.DataFrame | pa.Table:
        print("Start request to BigQuery")
        if self.bq_storage_client is not None:
            if data_format == DataFormat.pandas:
                df = self.bq_client.query(sql, project=self.project_name).to_dataframe(
                    bqstorage_client=self.bq_storage_client)
            elif data_format == DataFormat.arrow:
                df = self.bq_client.query(sql, project=self.project_name).to_arrow(
                    bqstorage_client=self.bq_storage_client)
        else:
            if data_format == DataFormat.pandas:
                df = self.bq_client.query(
                    sql, project=self.project_name).to_dataframe()
            elif data_format == DataFormat.arrow:
                df = self.bq_client.query(
                    sql, project=self.project_name).to_arrow()
        return df

    def bq_to_df(self, sql: str) -> pd.DataFrame:
        return self.__bq_to(sql, DataFormat.pandas)

    def bq_to_arrow(self, sql: str) -> pa.Table:
        return self.__bq_to(sql, DataFormat.arrow)

    def __bq_cache_to(self, sql: str, data_format: DataFormat, cache_table_id: str, clear_cache=False):
        try:
            self.bq_client.get_table(cache_table_id)
            print("Cache table {} already exists.".format(cache_table_id))
            if clear_cache:
                self.bq_client.delete_table(cache_table_id)
                print("Delete cache table {}".format(cache_table_id))
                raise NotFound("Delete cache table")
            else:
                sql = "select * from `{}`".format(cache_table_id)
                if data_format == DataFormat.pandas:
                    return self.bq_to_df(sql)
                elif data_format == DataFormat.arrow:
                    return self.bq_to_arrow(sql)
        except NotFound:
            print("Create cache table {}".format(cache_table_id))
            job_config = bigquery.QueryJobConfig(
                destination=cache_table_id, write_disposition="WRITE_EMPTY")
            query_job = self.bq_client.query(
                sql, project=self.project_name, job_config=job_config)
            if data_format == DataFormat.pandas:
                return query_job.to_dataframe()
            elif data_format == DataFormat.arrow:
                return query_job.to_arrow()

    def bq_cache_to_df(self, sql: str, cache_table_id: str, clear_cache=False) -> pd.DataFrame:
        return self.__bq_cache_to(sql, DataFormat.pandas, cache_table_id=cache_table_id, clear_cache=clear_cache)

    def bq_cache_to_arrow(self, sql: str, cache_table_id: str, clear_cache=False) -> pa.Table:
        return self.__bq_cache_to(sql, DataFormat.arrow, cache_table_id=cache_table_id, clear_cache=clear_cache)
