from fastapi import FastAPI, Depends
import app.models.table_model as models
from app.schemas.schemas import *
from app.databases.database import engine,get_db
from sqlalchemy.orm import Session
import uvicorn, json,yaml,time,requests

#Let us create a FastAPI instance
app = FastAPI()

#Let us now call a method that creates our tables defined inside 'models'
models.Base.metadata.create_all(bind=engine)
#Argument  -- bind -- it is our 'engine'
#Once this statement is run, all the tables are created inside the database of PostgreSQL


@app.post("/project/create",tags = ["Project"])
def create_row(proj:ProjectRow,db: Session = Depends(get_db)):
    """
    It takes in a ProjectRow object and creates a new row in the database
    
    :param proj: ProjectRow - This is the object that we will be passing to the function
    :type proj: ProjectRow
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: A dictionary with the key "Message" and the value "Project Created Successfully"
    """
    try:
        project_dict = proj.dict()
        new_row = models.ProjectEntity(**project_dict)
        proj_details = db.query(models.ProjectEntity).filter(models.ProjectEntity.project_name == proj.project_name)
        if(not proj_details.count()):
            #Let us now add this row to the table
            db.add(new_row)
        
            #Let us now commit the changes made
            db.commit()
            db.refresh(new_row)
            return {"Message":"Project Created Successfully","Project Details": new_row}
        else:
            return {"Message: Project creation failed. Please give a valid Project name."}
    except Exception as e:
        return {f"Error: {e}","Message: Project Creation Failed"}

@app.post("/project/delete",tags=["Project"])
def delete_row(row:ProjectRow,db: Session = Depends(get_db)):
    """
    We are deleting a row from the table
    
    :param row: ProjectRow
    :type row: ProjectRow
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: A dictionary is being returned.
    """
    try:
        #We are querying the table
        query = db.query(models.ProjectEntity)
        my_row = query.filter(models.ProjectEntity.project_name == row.project_name)
        
        #Let us now delete the row
        #For this, we use 'delete()'
        my_row.delete(synchronize_session=False)
        #Argument -- synchronize_session = False
        
        #Let us now commit the changes
        db.commit()
        return {"Message: Project Deleted Successfully"}
    except Exception as e:
        return {f"Error: {e}","Message: Project Deletion Failed"}

@app.post("/task/create",tags = ["Task"])
def create_row(row:TaskRow,db: Session = Depends(get_db)):
    """
    It creates a new row in the Task table if the project name is valid and the task name is unique
    
    :param row: TaskRow - This is the input parameter that is passed to the function
    :type row: TaskRow
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: A dictionary with a key and value.
    """
    try:
        task_dict = row.dict()
        new_row = models.TaskEntity(**task_dict)
        proj_row = db.query(models.ProjectEntity).filter(models.ProjectEntity.project_name == row.project_name)
        if(proj_row.count()):
            task_row = db.query(models.TaskEntity).filter(models.TaskEntity.project_name == row.project_name).filter(models.TaskEntity.task_name == row.task_name)
            if(not task_row.count()):            
                db.add(new_row)
                db.commit()
                db.refresh(new_row)
                return {"Message": "Task Created Successfully",
                "Task Details": new_row,
                "Project Details":task_row.first()}
            else:
                return {"Message: Task creation failed. Please give a valid Task name."}
        else:
            return {"Message: Task creation failed. Please give a valid Project name."}
                
    except Exception as e:
        return {f"Error: {e}","Message: Task Creation Failed"}

@app.post("/task/delete",tags=["Task"])
def delete_row(row:TaskRow,db: Session = Depends(get_db)):
    """
    We are querying the table, filtering the row we want to delete, deleting it, and committing the
    changes to the database
    
    :param row: This is the row that we want to delete
    :type row: TaskRow
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: A dictionary with a message
    """
    try:
        #We are querying the table
        query = db.query(models.TaskEntity)
        my_row = query.filter(models.TaskEntity.task_name == row.task_name)
        my_row.delete(synchronize_session=False)
        db.commit()
        return {"Message: Task Deleted Successfully"}
    except Exception as e:
        return {f"Error: {e}","Message: Task Deletion Failed"}

#_____________________________________________________________________________________________________________________________
#_____________________________________________________________________________________________________________________________
#SNOWFLAKE METADATA

#At this point, ingestion from ingestion service should be run
#Let us now save the result of the ingestion process to a table
#This API is run immediately after the ingestion process is completed.
#It is best if we run this inside the inegstion process itself.
@app.post("/ingest/ingest_result",tags = ["Metadata Ingestion"])
def ingest_results(details:MetadataIngestRow,db: Session = Depends(get_db)):
    """
    It takes a MetadataIngestRow object, converts it to a dictionary, creates a new
    MetadataIngestionEntity object from the dictionary, adds it to the database, commits the changes,
    refreshes the object, and returns a message
    
    :param details: MetadataIngestRow - this is the object that is passed in from the API call
    :type details: MetadataIngestRow
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: A tuple of strings
    """
    try:
        details_dict = details.dict()
        new_row = models.MetadataIngestionEntity(**details_dict)
        db.add(new_row)
        db.commit()
        db.refresh(new_row)
        return {"Message: Added Successfully",f"Details:{new_row}"}
    except Exception as e:
        return{f"Error: {e}", "Message: Addition Failed"}


#Create an API to save user details
@app.post("/ingest/snowflake/metadata_details",tags=['Snowflake Ingestion'])
def ingest_snow_meta(details:SnowflakeUserDetails,db: Session = Depends(get_db)):
    """
    It takes in a `SnowflakeUserDetails` object, and saves it to the database
    
    :param details: This is the object that is passed in the request body
    :type details: SnowflakeUserDetails
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: a dictionary with a key "Message" and a value "User details saved successfully"
    """
    try:
        
        # details_dict = details.dict(exclude={'project_name','task_name'})
        details_dict = details.dict()

        details_dict["dbservice_name"] = f"{details.project_name}||{details.task_name}||{details.dbservice_name}"
        
        proj_row = db.query(models.ProjectEntity).filter(models.ProjectEntity.project_name == details.project_name)
        if(proj_row.count()):
            task_row = db.query(models.TaskEntity).filter(models.TaskEntity.project_name == details.project_name).filter(models.TaskEntity.task_name == details.task_name)
            if(task_row.count()):
                versions = db.query(models.UserDetailsIngestion).filter(models.UserDetailsIngestion.dbservice_fqn == details_dict["dbservice_name"])
                final_dict = {
                    "project_name":details.project_name,
                    "task_name":details.task_name,
                    "user_details":json.dumps(details_dict),
                    "dbservice_fqn":details_dict["dbservice_name"],
                    "version":f"v{versions.count()+1}"
                }
    
                new_row = models.UserDetailsIngestion(**final_dict)
                db.add(new_row)
                db.commit()
                db.refresh(new_row)
                return {"Message": "User details saved successfully","dbservice_fqn":final_dict["dbservice_fqn"]}
            else:
                return {"Message: User details not saved. Please give a valid Task name."}
        else:
            return {"Message: User details not saved. Please give a valid Project name."}

    except Exception as e:
        return {"Error": f"{e}","Message": "User details not saved"}

#Let us return the row which contains the details of the metadata to be ingested
@app.post("/ingest/metadata/choose_Row",tags=["Metadata Ingestion"])
def ingest_metadata(dbservice_fqn:str,db: Session = Depends(get_db)):
    """
    It returns the latest metadata row for a given database service
    
    :param dbservice_fqn: This is the name of the database service that you want to ingest metadata from
    :type dbservice_fqn: str
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: The return value is a dict with two keys.
    """
    try:
        meta_row = db.query(models.UserDetailsIngestion).filter(models.UserDetailsIngestion.dbservice_fqn == dbservice_fqn)
        if(meta_row.count()):
            all_rows = meta_row.order_by(models.UserDetailsIngestion.created_at.desc())
            return all_rows.first()
        else:
            return {"Message":"No databaseService exists by the given name."}
    except Exception as e:
        return {f"Error: {e}","Message: Metadata Ingestion not possible."}

#Create a Yaml file
@app.post("/ingest/snowflake/create_yaml",tags = ["Snowflake Ingestion"])
def ingest_metadata(details:SnowflakeIngestionYaml):
    """
    It takes in a dictionary of details and creates a yaml file with the details
    
    :param details: SnowflakeIngestionYaml
    :type details: SnowflakeIngestionYaml
    :return: A dictionary with two keys: Message and Path.
    """
    ingestion_dict = {
        "source": {
            "type":'snowflake',
            "serviceName": details.dbservice_name,
            "serviceConnection": {
            "config": {
                "type": "Snowflake",
                "account": details.host,
                "username": details.username,
                "password": details.password,
                "database": details.database,
                "warehouse": details.warehouse
                
            }
            },
            "sourceConfig": {
            "config":{
                "includeTables": details.include_tables,
                
                "includeViews":  details.include_views,
                "schemaFilterPattern":{
                "includes": [f"{details.schema_pattern}.*"]
                }
                
            
                
            }
            }
        },
        "sink": {
            "type": "metadata-rest",
            "config": {}
        },
        "workflowConfig": {
            "openMetadataServerConfig": {
            "hostPort": "http://localhost:8585/api",
            "authProvider": "no-auth"
            }
        }
        }
    ingestion_yaml = yaml.dump(data=ingestion_dict)
    try:
        with open(f"/tmp/{details.file_name}.yaml","w") as f:
            f.write(ingestion_yaml)
        yaml_path = {"path":f"/tmp/{details.file_name}.yaml"}
        return {"Message":"Yaml Successfully Created", "Path":yaml_path}
        # return on_success_response(201,"Successfully written to Yaml",data=yaml_path)
    except:
        # return on_error_response(401,"Write Operation Failed",data=None)
        return {"Message":"Yaml Creation Failed"}



#_____________________________________________________________________________________________________________________________
#_____________________________________________________________________________________________________________________________
#SNOWFLAKE USAGE
@app.post("/ingest/snowflake/usage_details",tags=['Snowflake Usage'])
def ingest_snow_meta(details:SnowflakeUsageDetails,db: Session = Depends(get_db)):
    """
    It takes in a `SnowflakeUsageDetails` object, and saves it to the database
    
    :param details: This is the object that is passed in the request body
    :type details: SnowflakeUsageDetails
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: A dictionary with a key of "Message" and a value of "User details saved successfully"
    """
    try:
        
        # details_dict = details.dict(exclude={'project_name','task_name'})
        details_dict = details.dict()
        details_dict["dbservice_name"] = f"{details.project_name}||{details.task_name}||{details.dbservice_name}"
        
        proj_row = db.query(models.ProjectEntity).filter(models.ProjectEntity.project_name == details.project_name)
        if(proj_row.count()):
            task_row = db.query(models.TaskEntity).filter(models.TaskEntity.project_name == details.project_name).filter(models.TaskEntity.task_name == details.task_name)
            if(task_row.count()):
                versions = db.query(models.UserDetailsUsageIngestion).filter(models.UserDetailsUsageIngestion.dbservice_fqn == details_dict["dbservice_name"])
                final_dict = {
                    "project_name":details.project_name,
                    "task_name":details.task_name,
                    "user_details":json.dumps(details_dict),
                    "dbservice_fqn":details_dict["dbservice_name"],
                    "version":f"v{versions.count()+1}"
                }
    
                new_row = models.UserDetailsUsageIngestion(**final_dict)
                db.add(new_row)
                db.commit()
                db.refresh(new_row)
                return {"Message": "User details saved successfully","dbservice_fqn":final_dict["dbservice_fqn"]}
            else:
                return {"Message: User details not saved. Please give a valid Task name."}
        else:
            return {"Message: User details not saved. Please give a valid Project name."}

    except Exception as e:
        return {f"Error: {e}","Message: User details not saved"}

@app.post("/ingest/snowflake/usage/create_yaml",tags = ["Snowflake Usage"])
def ingest_usage(details:SnowflakeUsageYaml):
    """
    It takes in a dictionary of details and creates a yaml file in the /tmp directory
    
    :param details: SnowflakeUsageYaml = SnowflakeUsageYaml(
    :type details: SnowflakeUsageYaml
    :return: A dictionary with two keys: Message and Path.
    """
    usage_dict = {
            "source": {
                "type": "snowflake-usage",
                "serviceName": details.dbservice_name,
                "serviceConnection": {
                    "config": {
                        "type": "Snowflake",
                        "account": details.host,
                        "username": details.username,
                        "password": details.password,
                        "database": details.database,
                        "warehouse": details.warehouse
                    }
                },
                "sourceConfig": {
                    "config": {
                        "queryLogDuration": details.queryLogDuration,
                        "resultLimit": details.resultLimit
                    }
                }
            },
            "processor": {
                "type": "query-parser",
                "config": {}
            },
            "stage":{
                "type": "table-usage",
                "config":{
                    "filename": "/tmp/snowflake_usage"
                }
                
            },
            "bulkSink": {
                "type": "metadata-usage",
                "config": {
                    "filename": "/tmp/snowflake_usage"
                }
            },
            "workflowConfig": {
                    "openMetadataServerConfig": {
                        "hostPort": "http://localhost:8585/api",
                        "authProvider": "no-auth"
                    }
                }
            }
        
    usage_yaml = yaml.dump(data=usage_dict)
    try:
        with open(f"/tmp/{details.file_name}.yaml","w") as f:
            f.write(usage_yaml)
        yaml_path = {"path":f"/tmp/{details.file_name}.yaml"}
        return {"Message":"Yaml Successfully Created", "Path":yaml_path}
        # return on_success_response(201,"Yaml Successfully Created",data=yaml_path)
    except:
        # return on_error_response(401,"Write Operation Failed",data=None)
        return {"Message":"Yaml Creation Failed"}

#Let us return the row which contains the details of the metadata to be ingested
@app.post("/ingest/usage/choose_Row",tags=["Usage Ingestion"])
def ingest_usage(dbservice_fqn:str,db: Session = Depends(get_db)):
    """
    It takes a database service name as input and returns the last ingested usage data for that database
    service
    
    :param dbservice_fqn: The fully qualified name of the database service
    :type dbservice_fqn: str
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: The return value is a dictionary with two keys.
    """
    try:
        meta_row = db.query(models.UserDetailsUsageIngestion).filter(models.UserDetailsUsageIngestion.dbservice_fqn == dbservice_fqn)
        if(meta_row.count()):
            all_rows = meta_row.order_by(models.UserDetailsUsageIngestion.created_at.desc())
            return all_rows.first()
        else:
            return {"Message":"No databaseService exists by the given name."}
    except Exception as e:
        return {f"Error: {e}","Message: Usage Ingestion not possible."}


@app.post("/ingest/usage_result",tags = ["Usage Ingestion"])
def ingest_results(details:UsageResultsRow,db: Session = Depends(get_db)):
    """
    It takes a UsageResultsRow object, converts it to a dictionary, creates a new UsageIngestionEntity
    object from the dictionary, adds it to the database, commits the changes, refreshes the object, and
    returns a message
    
    :param details: UsageResultsRow - this is the object that is passed in from the client
    :type details: UsageResultsRow
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: A dictionary with two keys.
    """
    try:
        details_dict = details.dict()
        new_row = models.UsageIngestionEntity(**details_dict)
        db.add(new_row)
        db.commit()
        db.refresh(new_row)
        return {"Message: Added Successfully",f"Details:{new_row}"}
    except Exception as e:
        return{f"Error: {e}", "Message: Addition Failed"}
#_____________________________________________________________________________________________________________________________
#_____________________________________________________________________________________________________________________________
@app.post("/profiler/snowflake/input_details",tags=['Snowflake Profiler and Data Quality'])
def snow_profiler_data(details:SnowflakeProfiler,db: Session = Depends(get_db)):
    """
    It takes in a JSON object, converts it to a dictionary, adds a new key-value pair to the dictionary,
    and then inserts the dictionary into a database table
    
    :param details: SnowflakeProfiler - This is the data that is passed to the API
    :type details: SnowflakeProfiler
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: The return value is a dict with two keys: Message and dbservice_fqn.
    """
    try:
        
        # details_dict = details.dict(exclude={'project_name','task_name'})
        details_dict = details.dict()

        details_dict["dbservice_name"] = f"{details.project_name}||{details.task_name}||{details.dbservice_name}"
        
        proj_row = db.query(models.ProjectEntity).filter(models.ProjectEntity.project_name == details.project_name)
        if(proj_row.count()):
            task_row = db.query(models.TaskEntity).filter(models.TaskEntity.project_name == details.project_name).filter(models.TaskEntity.task_name == details.task_name)
            if(task_row.count()):
                versions = db.query(models.UserDetailsProfiling).filter(models.UserDetailsProfiling.dbservice_fqn == details_dict["dbservice_name"])
                final_dict = {
                    "project_name":details.project_name,
                    "task_name":details.task_name,
                    "user_details":json.dumps(details_dict),
                    "dbservice_fqn":details_dict["dbservice_name"],
                    "version":f"v{versions.count()+1}"
                }
    
                new_row = models.UserDetailsProfiling(**final_dict)
                db.add(new_row)
                db.commit()
                db.refresh(new_row)
                return {"Message": "User details saved successfully","dbservice_fqn":final_dict["dbservice_fqn"]}
            else:
                return {"Message: User details not saved. Please give a valid Task name."}
        else:
            return {"Message: User details not saved. Please give a valid Project name."}

    except Exception as e:
        return {"Error": f"{e}","Message": "User details not saved"}

#Choose the latest row
@app.post("/profiler/choose_Row",tags=["Data Profiling and Quality"])
def ingest_metadata(dbservice_fqn:str,db: Session = Depends(get_db)):
    """
    It takes in a dbservice_fqn and returns the metadata of the most recent version of the database
    service
    
    :param dbservice_fqn: The name of the database service
    :type dbservice_fqn: str
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: The return type is a dict.
    """
    try:
        meta_row = db.query(models.UserDetailsProfiling).filter(models.UserDetailsProfiling.dbservice_fqn == dbservice_fqn)
        if(meta_row.count()):
            all_rows = meta_row.order_by(models.UserDetailsProfiling.created_at.desc())
            return all_rows.first()
        else:
            return {"Message":"No databaseService exists by the given name."}
    except Exception as e:
        return {f"Error: {e}","Message: Metadata Ingestion not possible."}

#Create Yaml
#Create Yaml
@app.post("/profiler/snowflake/create_yaml",tags = ["Snowflake Profiler and Data Quality"])
def profiling(details:SnowflakeProfilerYaml):
    """
    It takes in a dictionary of values and creates a yaml file with the values in the dictionary
    
    :param details: SnowflakeProfilerYaml
    :type details: SnowflakeProfilerYaml
    :return: a dictionary with two keys: Message and Path.
    """
    profiler_dict =  {'source': {'type': 'snowflake', 'serviceName': details.dbservice_name, 'serviceConnection': {'config': {'type': 'Snowflake', 'username': details.username, 'password': details.password, 'database': details.database, 'warehouse': details.warehouse, 'account': details.host
            }
        }, 'sourceConfig': {'config': {'type': 'Profiler', 'generateSampleData': True, 'fqnFilterPattern': {'includes': [f'{details.dbservice_name}.{details.database}'
                    ]
                }
            }
        }
    }, 'processor': {'type': 'orm-profiler', 'config': {'test_suite': {'name': details.test_suite_name, 'tests': [
                    {'table': f'{details.dbservice_name}.{details.database}.{details.testing_table_schema}.{details.testing_table_name}', 'profile_sample': 50, 'table_tests': [
                            {'testCase': {'tableTestType': details.first_table_test_type, 'config': {'value': details.first_table_test_value
                                    }
                                }
                            }
                        ], 'column_tests': [
                            {'columnName': details.testing_column_name, 'testCase': {'columnTestType': details.column_test_type, 'config': {'minValue': details.column_test_min_value, 'maxValue': details.column_test_max_value
                                    }
                                }
                            }
                        ]
                    }
                ]
            }
        }
    }, 'sink': {'type': 'metadata-rest', 'config': {}
    }, 'workflowConfig': {'openMetadataServerConfig': {'hostPort': 'http://localhost:8585/api', 'authProvider': 'no-auth'}}}
    profiler_yaml = yaml.dump(data=profiler_dict)
    try:
        with open(f"/tmp/{details.file_name}.yaml","w") as f:
            f.write(profiler_yaml)
        yaml_path = {"path":f"/tmp/{details.file_name}.yaml"}
        return {"Message":"Yaml Successfully Created", "Path":yaml_path}
        # return on_success_response(201,"Successfully written to Yaml",data=yaml_path)
    except:
        # return on_error_response(401,"Write Operation Failed",data=None)
        return {"Message":"Yaml Creation Failed"}

#Load output to postgres_db
@app.post("/profiler/profiling_result",tags = ["Data Profiling and Quality"])
def ingest_results(details:ProfilingResultsRow,db: Session = Depends(get_db)):
    """
    It takes a ProfilingResultsRow object and adds it to the database
    
    :param details: ProfilingResultsRow - this is the object that is passed in from the API call
    :type details: ProfilingResultsRow
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: The return value is a tuple of two strings.
    """
    try:
        details_dict = details.dict()
        new_row = models.ProfilingEntity(**details_dict)
        db.add(new_row)
        db.commit()
        db.refresh(new_row)
        return {"Message: Added Successfully",f"Details:{new_row}"}
    except Exception as e:
        return{f"Error: {e}", "Message: Addition Failed"}


# _______________________________________________________________________________________________________
#_____________________________________________________________________________________________________________________________
#DRIFT SERVICE
@app.post("/drift/input_details",tags =["Drift"])
def drift_service(details:FeatureDriftInputDetails,db: Session = Depends(get_db)):
    """
    > The function takes in a FeatureDriftInputDetails object and a database session object as input and
    returns a dictionary with the message and the fully qualified name of the drift service
    
    :param details: FeatureDriftInputDetails - This is the input parameter that is passed to the
    function. It is a Pydantic model
    :type details: FeatureDriftInputDetails
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: A dictionary with a key "Message" and a value "User details saved successfully"
    """
    try:
        
        details_dict = details.dict(exclude={'project_name','task_name'})

        details_dict["driftservice_fqn"] = f"{details.project_name}.{details.task_name}.{details.drift_type}"
        
        proj_row = db.query(models.ProjectEntity).filter(models.ProjectEntity.project_name == details.project_name)
        if(proj_row.count()):
            task_row = db.query(models.TaskEntity).filter(models.TaskEntity.project_name == details.project_name).filter(models.TaskEntity.task_name == details.task_name)
            if(task_row.count()):
                versions = db.query(models.DriftServiceDetails).filter(models.DriftServiceDetails.driftservice_fqn == details_dict["driftservice_fqn"])
                final_dict = {
                    "project_name":details.project_name,
                    "task_name":details.task_name,
                    "input_details":json.dumps(details_dict),
                    "driftservice_fqn":details_dict["driftservice_fqn"],
                    "version":f"v{versions.count()+1}"
                }
    
                new_row = models.DriftServiceDetails(**final_dict)
                db.add(new_row)
                db.commit()
                db.refresh(new_row)
                return {"Message": "User details saved successfully","driftservice_fqn":final_dict["driftservice_fqn"]}
            else:
                return {"Message: User details not saved. Please give a valid Task name."}
        else:
            return {"Message: User details not saved. Please give a valid Project name."}

    except Exception as e:
        return {f"Error: {e}","Message: User details not saved"}

@app.post("/drift/output_details",tags = ["Drift"])
def drift_dumps(details:DriftDumpRow,db: Session = Depends(get_db)):
    """
    It takes a row of data from the DriftDumpRow class, and adds it to the DriftService_Dump table in
    the database
    
    :param details: DriftDumpRow = Body(..., embed=True)
    :type details: DriftDumpRow
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: a dictionary with a key and value.
    """
    try:
        details_dict = details.dict()
        new_row = models.DriftService_Dump(**details_dict)
        db.add(new_row)
        db.commit()
        db.refresh(new_row)
        return {"Message": "Successfully saved the output"}
           
    except Exception as e:
        return {f"Error: {e}","Message: Output not saved"}

#choose latest row
@app.post("/drift/choose_Row",tags=["Drift"])
def choose_latest_row(driftservice_fqn:str,db: Session = Depends(get_db)):
    """
    It takes a string as input, and returns the latest row from the database table, where the column
    "driftservice_fqn" matches the input string
    
    :param driftservice_fqn: This is the name of the service that you want to get the latest row for
    :type driftservice_fqn: str
    :param db: Session = Depends(get_db)
    :type db: Session
    :return: A single row from the database.
    """
    try:
        meta_row = db.query(models.DriftServiceDetails).filter(models.DriftServiceDetails.driftservice_fqn == driftservice_fqn)
        if(meta_row.count()):
            all_rows = meta_row.order_by(models.DriftServiceDetails.created_at.desc())
            return all_rows.first()
        else:
            return {"Message":"No Service exists by the given name."}
    except Exception as e:
        return {f"Error: {e}","Message: Fetching latest row failed."}


#______________________________________________________________________________________________________________________
#_____________________________________________________________________________________________________________________________


if(__name__=="__main__"):
    uvicorn.run("main:app",reload=True)