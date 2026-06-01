from __future__ import annotations

import json
import logging
import os
import threading
import traceback
from datetime import datetime
from typing import Any, Dict, List, Tuple, Type

import pandas as pd
from dotenv import load_dotenv
from google.auth import compute_engine
from google.cloud import bigquery
from .bigquery_loader import BigQueryLoader

load_dotenv()

logger = logging.getLogger(__name__)


class BigQueryHandler:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, singleton: bool = True, **kwargs):
        if not singleton:
            # Non-singleton mode: build a fresh, isolated instance that does
            # not touch (or read from) the process-wide cached instance.
            instance = super(BigQueryHandler, cls).__new__(cls)
            instance._initialize(*args, **kwargs)
            return instance
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(BigQueryHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(
        self,
        serviceAccountJson: dict | None = None,
        client: bigquery.Client | None = None,
    ):
        if getattr(self, "_initialized", False):
            logger.debug("BigQueryHandler already initialized, skipping")
            return

        logger.info("Initializing BigQueryHandler")

        try:
            if client:
                logger.info("Initializing BigQuery client via provided client")
                self.client = client
            elif serviceAccountJson:
                logger.info("Initializing BigQuery client via provided serviceAccountJson")
                self.client = bigquery.Client.from_service_account_info(serviceAccountJson)
            else:
                load_dotenv()
                sa_json = os.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON")
                if sa_json:
                    logger.info("Initializing BigQuery client via env var: BIGQUERY_SERVICE_ACCOUNT_JSON")
                    credentials = json.loads(sa_json)
                    self.client = bigquery.Client.from_service_account_info(credentials)
                    logger.info("Successfully initialized BigQuery client via env var: BIGQUERY_SERVICE_ACCOUNT_JSON")
                elif os.path.exists("var/bigquery_service_account.json"):
                    logger.info("Initializing BigQuery client via file: var/bigquery_service_account.json")
                    self.client = bigquery.Client.from_service_account_json(
                        "var/bigquery_service_account.json"
                    )
                    logger.info("Successfully initialized BigQuery client via file: var/bigquery_service_account.json")
                else:
                    logger.info("Initializing BigQuery client via compute engine default credentials")
                    credentials = compute_engine.Credentials()
                    self.client = bigquery.Client(credentials=credentials)
                    logger.info("Successfully initialized BigQuery client via compute engine default credentials")

            self.Loader = BigQueryLoader(client=self.client)
            self.project_id = self.client.project
            self._initialized = True
            logger.info(f"BigQueryHandler initialized successfully with project_id: {self.project_id}")

        except Exception as e:
            if hasattr(self, "client"):
                del self.client
            logger.exception(f"Error initializing BigQueryHandler: {e}")
            raise RuntimeError("Error initializing GCP credentials") from e

    def formatParams(self, params: list[dict]) -> list[dict]:
        dateFormatList = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S.%f",
            "%m/%d/%Y",
        ]
        try:
            for param in params:
                if param["Type"] == "DATE":
                    for fmt in dateFormatList:
                        try:
                            param["Value"] = datetime.strptime(param["Value"], fmt).strftime("%Y-%m-%d")
                            break
                        except Exception:
                            continue
                elif param["Type"] == "TEXT":
                    param["Type"] = "STRING"
            return params
        except Exception:
            print(traceback.format_exc())
            return params

    def runQuery(self, query: str, useLegacySql: bool = False) -> Tuple[pd.DataFrame, str]:
        try:
            queryJob = self.client.query(
                query,
                job_config=bigquery.QueryJobConfig(use_legacy_sql=useLegacySql),
            )
            jobId = queryJob.job_id
            results = queryJob.result().to_dataframe()
            print(f"Query job ID: {jobId}")
            print(f"Retrieved {len(results)} rows")
            return results, jobId
        except Exception as e:
            raise Exception(f"Error running query: {e}")

    def getTable(
        self,
        datasetId: str,
        tableId: str,
        format: Type = pd.DataFrame,
    ) -> pd.DataFrame | list[dict]:
        try:
            table_ref = self.client.dataset(datasetId).table(tableId)
            table = self.client.get_table(table_ref)
            if format is list:
                return [dict(row) for row in self.client.list_rows(table)]
            df = self.client.list_rows(table).to_dataframe()
            print(f"Retrieved {len(df)} rows from {tableId} in {datasetId}")
            return df
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error retrieving table {tableId} in dataset {datasetId}: {e}")

    def retrieveJobDetails(self, bigQueryJobs: dict) -> list:
        try:
            jobIds = list(bigQueryJobs.keys())
            if not jobIds:
                return []
            jobIdString = ",".join(f'"{id}"' for id in jobIds)
            query = f"""
            SELECT
                creation_time, project_id, project_number, job_id, job_type,
                statement_type, priority, start_time, end_time, query, state
            FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
            WHERE job_id IN ({jobIdString})
            """
            job = self.runQuery(query)
            return [dict(row) for row in job]  # type: ignore
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error retrieving job details: {e}")

    def retrieveDatasetTableFunctions(self, datasetId: str) -> list:
        try:
            query = f"""
                SELECT *
                FROM (
                    SELECT
                        specific_name,
                        REGEXP_EXTRACT(ddl,'.*description="(.*)".*') AS Name
                    FROM `{self.client.project}.{datasetId}.INFORMATION_SCHEMA.ROUTINES`)
                LEFT JOIN `{self.client.project}.{datasetId}.INFORMATION_SCHEMA.PARAMETERS` USING(specific_name)
            """
            results, _ = self.runQuery(query)
            tableFunctions = {}
            for row in results.itertuples():
                functionName = row.specific_name  # type: ignore
                reportName = row.Name  # type: ignore
                paramPosition = row.ordinal_position  # type: ignore
                paramName = row.parameter_name  # type: ignore
                paramType = row.data_type  # type: ignore
                tableFunctions.setdefault(functionName, {}).update({
                    "FunctionName": functionName,
                    "ReportName": reportName,
                    "ReportId": functionName.split("_")[-1],
                })
                if paramName:
                    tableFunctions[functionName].setdefault("Params", []).append({
                        "Position": paramPosition,
                        "Name": paramName,
                        "Type": paramType,
                    })
                else:
                    tableFunctions[functionName].setdefault("Params", [])
            return [value for _, value in tableFunctions.items()]
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error retrieving dataset functions: {e}")

    def runTableFunction(self, datasetId: str, selectedReport: dict, params: dict) -> list:
        try:
            tableFunctionName = selectedReport["FunctionName"]
            paramList = sorted(params.values(), key=lambda x: x["Position"])
            paramList = self.formatParams(paramList)
            query = (
                f"SELECT * FROM `{self.client.project}.{datasetId}.{tableFunctionName}`("
                + ",".join(f"'{param['Value']}'" for param in paramList)
                + ")"
            )
            return self.runQuery(query)  # type: ignore
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error running table function: {e}")

    def checkIfTableFunctionExists(self, tableFunctions: list, reportId: int) -> bool:
        try:
            return bool([tf for tf in tableFunctions if tf["ReportId"] == str(reportId)])
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error checking if table function exists: {e}")

    def checkTableExists(self, tableName: str, datasetId: str) -> bool:
        try:
            self.client.get_table(self.client.dataset(datasetId).table(tableName))
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Bulq-specific helpers
    # ------------------------------------------------------------------

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        dataset_id: str,
        table_id: str,
        *,
        write_disposition: str = "WRITE_APPEND",
    ) -> bigquery.LoadJob:
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True,
        )
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Loaded {len(df)} rows into {table_ref}")
        return job

    def insert_rows(
        self,
        dataset_id: str,
        table_id: str,
        rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        table_ref = self.client.dataset(dataset_id).table(table_id)
        errors = self.client.insert_rows_json(table_ref, rows)
        if errors:
            print(f"BQ insert errors for {dataset_id}.{table_id}: {errors}")
        else:
            print(f"Inserted {len(rows)} rows into {dataset_id}.{table_id}")
        return list(errors)
