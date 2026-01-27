from google.cloud import bigquery
import copy
import traceback
from .schema_generator import SchemaGenerator
import json
from datetime import datetime
import pandas as pd
import regex as re
import numpy as np

class BigQueryLoader:
    """
    Helper class for loading data into BigQuery.

    Handles schema generation, JSON formatting, duplicate removal, and data loading
    from both JSON/Dictionaries and Pandas DataFrames.
    """
    def __init__(self,client = None,serviceAccountJson = None, credentials = None, debug = False):
        """
        Initializes the BigQueryLoader class.

        Args:
            client (bigquery.Client, optional): Existing BQ client.
            serviceAccountJson (dict, optional): Service account credentials.
            credentials (google.auth.credentials.Credentials, optional): Auth credentials.
            debug (bool): Whether to run in debug mode (skips actual load).
        """
        
        self.client = self.createBQClient(client, serviceAccountJson, credentials)
        self.SchemaGenerator = SchemaGenerator()
        self.debug = debug

    def createBQClient(self, client = None,serviceAccountJson = None, credentials = None) -> bigquery.Client:
        """
        Creates a BigQuery client using the provided service account credentials.

        """
        try:
            if client:
                print('Using provided BigQuery client...')
                self.BQproject = client.project
                return client
            elif serviceAccountJson:
                print('Creating BigQuery client with service account...')
                self.BQproject = serviceAccountJson['project_id']
                client = bigquery.Client.from_service_account_info(serviceAccountJson)
            
            elif credentials:
                print('Creating BigQuery client with credentials...')
                client = bigquery.Client(project=self.BQproject, credentials=credentials)
                self.BQproject = client.project
            else:
                print('No credentials provided')
                raise ValueError("No credentials provided")
            print('BigQuery client created')
            return client
        except Exception as e:
            print('Error creating BigQuery client')
            traceback.print_exc()
            raise e

    def removeDuplicatesFields(self,data : list) -> list:
        """
        Removes duplicate fields from the JSON data.
        """
        def recRemoveDuplicatesFields(json : dict) -> dict:
            if isinstance(json,dict):
                new_json = {}
                visited = {}
                for (field) in json:
                    if visited.get(str(field).lower(),False):
                        continue
                    else:
                        new_json[field.replace('-','_').replace('\r','').replace(' ','').replace('$','').replace('\ufeff','').replace('\"','')] = recRemoveDuplicatesFields(json[field])
                        visited[str(field).lower()] = True
                return new_json
            elif isinstance(json,list):
                return [recRemoveDuplicatesFields(unit) for unit in json]
            elif isinstance(json,str):
                return json.replace('\r','')
            else:
                return json
        return recRemoveDuplicatesFields(data)
    
    def formatJSON(self, data : list, platform : str) -> list:
        """
        Formats the JSON data to remove duplicates and convert version fields to integers.

        Args:
            data (list): The JSON data to be formatted.
            platform (str): The platform name.
        Returns:
            list: The formatted JSON data.
        """
        new_data = copy.deepcopy(data)
        def recFunc(dictio):
            if not isinstance(dictio, dict):
                return dictio

            for field in list(dictio.keys()):
                value = dictio[field]

                # Skip None early
                if value is None:
                    continue

                # Convert version field to int safely
                if platform not in ('masonhub', 'shopify') and field == 'version':
                    try:
                        dictio[field] = int(eval(value))
                    except Exception:
                        dictio[field] = value
                    continue

                # Handle timestamps
                if isinstance(value, (pd.Timestamp, datetime)):
                    dictio[field] = value.isoformat()
                    continue

                # Handle nested dicts
                if isinstance(value, dict):
                    dictio[field] = None if value == {} else recFunc(value)
                    continue

                # Handle numpy arrays
                if isinstance(value, np.ndarray):
                    if value.size == 0:
                        dictio[field] = None
                        continue
                    # ✅ Fix: handle 0-D arrays (non-iterable)
                    if value.ndim == 0:
                        dictio[field] = value.item()
                        continue
                    # ✅ Only iterate if it's truly an array of dicts
                    if all(isinstance(unit, dict) for unit in value.tolist()):
                        dictio[field] = [recFunc(unit) for unit in value.tolist()]
                    else:
                        dictio[field] = value.tolist()
                    continue

                # Handle lists
                if isinstance(value, list):
                    if value == [{}] or len(value) == 0:
                        dictio[field] = None
                        continue
                    if all(isinstance(unit, dict) for unit in value):
                        dictio[field] = [recFunc(unit) for unit in value]
                    else:
                        dictio[field] = value
                    continue

                # Handle numpy scalars (e.g., np.int64, np.float64)
                if isinstance(value, (np.generic,)):
                    dictio[field] = value.item()
                    continue

                # Fallback: keep value as is
                dictio[field] = value

            return dictio

        for i,dictio in enumerate(new_data):
            # print(dictio)
            new_data[i] = recFunc(dictio)
        return new_data

    def getSchema(self,new_data : list,client : str,dataset_ref : str,BQtable : str ,base_table : str  = None,force_schema :bool = False,force_type : str = None) -> list:
        """
        Gets the schema of the table from BigQuery or generates it from the data.\n
        If the table already exists, it will use the schema of the existing table.\n
        If the table does not exist, it will generate the schema from the data.\n
        If force_schema is True, it will generate the schema from the data even if the table exists.\n
        If base_table is provided, it will use the schema of the base table.\n

        Args:
            new_data (list): The data to be loaded to BigQuery.
            client (str): The BigQuery client.
            dataset_ref (str): The dataset reference.
            BQtable (str): The table name.
            base_table (str): The base table name.
            force_schema (bool): Whether to force the schema generation.
        Returns:
            list: The schema of the table.
        """
        table_ref = dataset_ref.table(BQtable)
        if force_schema:
                print('Forcing schema...')
                schema = self.SchemaGenerator.generateSchema(new_data,force_type=force_type)
        else:   
            print('Getting schema...')
            if base_table is not None:
                try:
                    base_table_ref = dataset_ref.table(base_table)
                    client_base_table = client.get_table(base_table_ref)
                    print('Provided base table found, getting schema...')
                    schema = client_base_table.schema
                    return schema
                except Exception as e:      
                    print('Provided base table not found, generating schema...')
                    schema = self.SchemaGenerator.generateSchema(new_data,force_type=force_type)
                    # traceback.print_exc()
            else:
                if BQtable.endswith('_temp'):
                    new_BQtable = BQtable[:-5]
                    new_table_ref = dataset_ref.table(new_BQtable)
                    try:
                        client_new_table = client.get_table(new_table_ref)
                        print('Base table found, getting schema...')
                        schema = client_new_table.schema
                    except Exception as e:
                        print('No base table found, looking for temp table...')
                        try:
                            client_table = client.get_table(table_ref)
                            print('Temp table found, getting schema...')
                            schema = client_table.schema
                        except Exception as e:
                            schema = self.SchemaGenerator.generateSchema(new_data,force_type=force_type)
                            print('Temp table not found, generating schema...')
                else:
                    try:
                        client_table = client.get_table(table_ref)
                        print('Already a base table, getting schema...')
                        schema = client_table.schema
                    except Exception as e:
                        # traceback.print_exc()
                        schema = self.SchemaGenerator.generateSchema(new_data,force_type=force_type)
                        print('Table not found, generating schema...')
                
        return schema
    

    def enforce_schema_types(self, data : list, schema : list) -> list:
        """
        Preprocesses the data to ensure that all fields match their types in the schema.
        If a field is defined as STRING in the schema, non-string values are converted to strings.
        """

        def cast_value(value, field_type):
            try:   
                if field_type == "STRING":
                    return str(value) 
                elif field_type == "FLOAT":
                    return float(value)
                elif field_type == "INTEGER":
                    return int(value)
                elif field_type == "BOOLEAN":
                    return bool(value)
                else:
                    return value
            except Exception as e:
                print(f"Error casting value {value} to type {field_type}")
                return None

        def convert_value(value, field):
            if field.field_type == "RECORD" and not isinstance(value, dict):
                # Convert non-records to empty records
                return {key.name : cast_value(value,key.field_type) for key in field.fields}

            if field.field_type == "STRING" :
                if field.mode == "REPEATED":
                    # Convert each value in the list
                    return value if isinstance(value, list) else [str(item) for item in value]
                else:
                    # Convert non-string values to strings
                    return json.dumps(value) if isinstance(value, (dict)) else str(value)
            return value

        def process_record(record, schema_fields):
            processed_record = {}
            for field in schema_fields:
                field_name = field.name
                field_type = field.field_type  # Example: STRING, INTEGER, etc.
            
                if field_name in record:
                    value = record[field_name]
                    if value is not None:  # Only process non-null values
                        if field_type == "RECORD" and isinstance(value, dict):
                            # Handle nested RECORDs (recursively process subfields)
                            processed_record[field_name] = process_record(value, field.fields)
                        elif field_type == "RECORD" and isinstance(value, list):
                            # Handle repeated RECORDs (process each sub-record)
                            processed_record[field_name] = [
                                process_record(item, field.fields) for item in value
                            ]
                        else:
                            # Convert or validate the value
                            processed_record[field_name] = convert_value(value, field)
                    else:
                        # Preserve null values
                        processed_record[field_name] = None
                else:
                    # If the field is not present, set it to None
                    processed_record[field_name] = None
            return processed_record

        # Process each record in the data
        return [process_record(record, schema) for record in data]


    def checkDataset(self, dataset_ref: bigquery.DatasetReference) -> bool:
        try:
            self.client.get_dataset(dataset_ref)
            print('✅ Dataset found.')
            return True
        except Exception as e:
            print('⚠️ Dataset not found, attempting to create it...')
            try:
                dataset = bigquery.Dataset(dataset_ref)
                self.client.create_dataset(dataset)
                print('✅ Dataset created successfully.')
                return True
            except Exception as create_err:
                print('❌ Failed to create dataset.')
                traceback.print_exc()
                return False


    def loadJSONToBQ(self,data :list ,datasetId : list ,tableId : list ,platform : str = None,base_table : str = None,force_schema : bool = False,WRITE_DISPOSITION : str ='WRITE_TRUNCATE',force_type : str = None, include_loaded_at : bool = False) -> None:

        dataset_ref = self.client.dataset(datasetId, project = self.BQproject)
        table_ref = dataset_ref.table(tableId)  
        
        if not self.checkDataset(dataset_ref):
            print('Error creating dataset')
            raise Exception('Error creating dataset')

        print('Removing duplicates fields and formatting JSON...')
        
        now = datetime.now().isoformat()
        if include_loaded_at:
            for item in data:
                item['loaded_at'] = now

        new_data = self.removeDuplicatesFields(self.formatJSON(data,platform))
        schema = self.getSchema(new_data,self.client,dataset_ref,tableId,base_table,force_schema,force_type)
         
        
        new_data = self.enforce_schema_types(new_data, schema)
        
        # with open('schema.json','w') as f:
        #     f.write(json.dumps(self.SchemaGenerator.transformBigquerySchemaToJsonSchema(schema),indent=6))
        # with open('data.json','w') as f:
        #     f.write(json.dumps(new_data,indent=6))

        if not(self.debug):
            try:
                print('Loading data to BigQuery...')
                job_config = bigquery.job.LoadJobConfig(schema = schema, autodetect = False)
                if WRITE_DISPOSITION == "WRITE_TRUNCATE":
                    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
                elif WRITE_DISPOSITION == 'WRITE_APPEND':
                    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                job_config.create_disposition = bigquery.job.CreateDisposition.CREATE_IF_NEEDED
                job = self.client.load_table_from_json(new_data,table_ref,job_config = job_config,num_retries=10)
                print(job.result()) 
            except Exception as e:
                print('Error loading data to BigQuery, trying to force schema...')
                traceback.print_exc()
                schema = self.SchemaGenerator.generateSchema(new_data,schema)
                if WRITE_DISPOSITION == "WRITE_APPEND":
                    print("Updating schema of temp table...")
                    table_ref = dataset_ref.table(tableId)
                    client_table = self.client.get_table(table_ref)
                    client_table.schema = schema
                    self.client.update_table(client_table,['schema'])
                    print('Schema updated')
                job_config = bigquery.job.LoadJobConfig(schema = schema, autodetect = False)
                if WRITE_DISPOSITION == "WRITE_TRUNCATE":
                    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
                elif WRITE_DISPOSITION == 'WRITE_APPEND':
                    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                job_config.create_disposition = bigquery.job.CreateDisposition.CREATE_IF_NEEDED
                job = self.client.load_table_from_json(new_data,table_ref,job_config = job_config,num_retries=10)
                print(job.result()) 
                try:
                    if base_table is not None:
                        print('Updating schema of base table...')
                        base_table_ref = dataset_ref.table(base_table)
                        client_base_table = self.client.get_table(base_table_ref)
                        client_base_table.schema = schema
                        try:
                            self.client.update_table(client_base_table,['schema'])
                            print('Schema updated')
                        except Exception as e:
                            traceback.print_exc()
                            print('Error updating schema of base table')
                    elif BQtable.endswith('_temp'):
                            new_BQtable = BQtable[:-5]
                            new_table_ref = dataset_ref.table(new_BQtable)
                            try:
                                client_new_table = self.client.get_table(new_table_ref)
                                client_new_table.schema = schema
                                self.client.update_table(client_new_table,['schema'])
                                print('Schema updated')
                            except Exception as e:
                                print('Error updating schema of base table')
                                traceback.print_exc()
                except Exception as e:
                    print('No base table found')
                    traceback.print_exc
        else:
            print('Debug mode, not loading data to BigQuery')
            schema = self.SchemaGenerator.generateSchema(new_data,schema)
            print(schema)


    def formatDataframeColumnNames(self,dataframe : pd.DataFrame) -> pd.DataFrame:
        """
        Formats the column names of the DataFrame to be compatible with BigQuery.

        Args:
            dataframe (pd.DataFrame): The DataFrame to be formatted.

        Returns:
            pd.DataFrame: The formatted DataFrame.
        """
        def clean_column(col):
            col = col.replace('#', 'Number')
            col = col.replace('-', '_').replace(' ', '_').replace('/', '_')
            col = col.replace('(', '').replace(')', '')
            col = re.sub(r'[^a-zA-Z0-9_]', '', col)  # Remove all except letters, numbers, and _
            return col
        print(dataframe.columns)
        dataframe.columns = [clean_column(col) for col in dataframe.columns]
        print(dataframe.columns)
        return dataframe

    def loadDataframeToBigQuery(self,dataframe : pd.DataFrame,tableId : str,datasetId : str,WRITE_DISPOSITION : str ='WRITE_TRUNCATE', include_loaded_at : bool = False) -> None:
        """
        Loads a pandas DataFrame to BigQuery.

        Args:
            dataframe (pd.DataFrame): The DataFrame to be loaded.
            tableId (str): The name of the table in BigQuery.
            datasetId (str): The name of the dataset in BigQuery.
            base_table (str): The base table name.
            force_schema (bool): Whether to force the schema generation.
            WRITE_DISPOSITION (str): The write disposition for the load job.
        """

        dataset_ref = self.client.dataset(datasetId, project = self.BQproject) 
        table_id = f"{self.BQproject}.{datasetId}.{tableId}"

        if not self.checkDataset(dataset_ref):
            print('Error creating dataset')
            raise Exception('Error creating dataset')

 
        if not(self.debug):
            try:
                print('Loading data to BigQuery...')
                job_config = bigquery.job.LoadJobConfig()
                if WRITE_DISPOSITION == "WRITE_TRUNCATE":
                    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
                elif WRITE_DISPOSITION == 'WRITE_APPEND':
                    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                job_config.create_disposition = bigquery.job.CreateDisposition.CREATE_IF_NEEDED
                if include_loaded_at:
                    dataframe['loaded_at'] = pd.Timestamp.now().isoformat()
                job = self.client.load_table_from_dataframe(dataframe=self.formatDataframeColumnNames(dataframe),destination=table_id,job_config = job_config,num_retries=10)
                print(job.result()) 
            except Exception as e:
                print('Error loading data to BigQuery.')
                raise e
        else:
            print('Debug mode, not loading data to BigQuery')
            schema = self.SchemaGenerator.generateSchema(dataframe,schema)
            print(schema)