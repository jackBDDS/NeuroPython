"""
The spark_manager module provides functions to interact with the
Spark Manager in Neuroverse.
"""

from neuro_python.neuro_call import neuro_call
import uuid
import os
import re
import textwrap
import datetime
import pandas as pd
pd.set_option('display.max_rows', None)
import time
import threading
from IPython.display import display,HTML,TextDisplayObject
import ipywidgets as widgets
from typing import Union, Tuple
from functools import singledispatch

from IPython.core import magic_arguments
from IPython.core.magic import line_magic, cell_magic, line_cell_magic, Magics, magics_class

#Default context id
context_id=''

def script_parameter(name: str, value):
    """
    Cmd line parameter value to be used in the pyspark script. eg sys.argv[1]
    """
    return {"Name":name,"Value":value}

def check_and_break_while(start_time:datetime.datetime, max_time:int=1020, raise_error:bool=True, process:str = "Cluster boot-up") -> bool:
    """
    Check if the time taken is greater than the max_time (seconds), if so raise an error or return True.
    NOTE: Assumes start_time is a datetime.datetime object with timezone.utc.
    """
    check_time = (datetime.datetime.now(datetime.timezone.utc)-start_time).seconds
    if check_time > max_time and raise_error:
        raise TimeoutError(f'{process} taking longer than expected, please check status.')
    elif check_time > max_time and not raise_error:
        return True
    elif check_time <= max_time and raise_error:
        return False
    elif check_time <= max_time and not raise_error:
        return False
    else:
        raise ValueError(f'Unexpected error in check_and_break_while function for {process}, please check inputs.')

@singledispatch
def match_status(match: Union[list,str], status: str) -> bool:
    """
    Check if a status is in a list of possible statuses. This requires that the status is a substring of the match or vice versa.
    """
    raise ValueError(f"match_status input `match` must be list or str, got {type(match)}")

@match_status.register(str)
def _(match: str, status: str) -> bool:
    if isinstance(status, str):
        return status.lower() in match.lower() or match.lower() in status.lower()
    else:
        raise ValueError(f"match_status input `status` must be str, got {type(status)}")

@match_status.register(list)
def _(match: list, status: str) -> bool:
    if isinstance(status, str):
        if not all(isinstance(i, str) for i in match):
            raise ValueError(f"match list elements must all be lists, got {match}")
        return any([status.lower() in match_i.lower() or match_i.lower() in status.lower() for match_i in match])
    else:
        raise ValueError(f"match_status input `status` must be str, got {type(status)}")

def import_table(dataframe_name: str, data_store_name: str, table_name: str, partition_paths: "List[str]" = ["'/'"], sql_query: str = None, ignore_non_existing_partition_paths: bool = None):
    """
    Neuroverse datalake data to be used in the pyspark script.
    partition_paths contains a list of string that can be feed into the python eval function and return a str.
    This allows for the partition that is used in a spark job run to be determined at runtime (eg. using the current datetime).
    If you want to use a constant partition path string must be wrapped in quotes. eg "'/2018/1'"
    """
    if partition_paths!=None:
        for path in partition_paths:
            if not isinstance(eval(path), str):
                raise Exception("A string is not returned when evaluating: " + path)
    return {"SparkDataFrameName":dataframe_name, "DataStoreName":data_store_name, "TableName":table_name, "PartitionPaths":partition_paths, "SqlQuery":sql_query, "IgnoreNonExistingPaths":ignore_non_existing_partition_paths}
  
def export_table(dataframe_name: str, data_store_name: str, table_name: str, partition_path: str = "'/'", write_mode: str = "append"):
    """
    pyspark script data to be outputed into a Neuroverse datalake.
    partition_path contains string that can be feed into the python eval function and return a str.
    This allows for the partition that is used in a spark job run to be determined at runtime (eg. using the current datetime).
    If you want to use a constant partition path string must be wrapped in quotes. eg "'/2018/1'"
    """
    if partition_path!=None:
        if not isinstance(eval(partition_path), str):
            raise Exception("A string is not returned when evaluating: " + partition_path)
    if write_mode!=None:
        if write_mode!="append" and write_mode!="overwrite":
            raise Exception("Invalid write_mode: write_mode must either be append or overwrite")
    return {"SparkDataFrameName":dataframe_name, "DataStoreName":data_store_name, "TableName":table_name, "PartitionPath":partition_path, "WriteMode":write_mode}

def library(library_name: str, library_type: int = 0, workspace_id: str = None, cluster_id: str = None):
    libraries=sorted([i for i in list_libraries(workspace_id=workspace_id,cluster_id=cluster_id,show_all=True) if i['LibraryName']==library_name],key=lambda x:x['LibraryVersion'])
    if len(libraries)==0:
        raise Exception("Library not found")
    return {'LibraryName' : library_name, 'LibraryType' : library_type, 'LibraryVersion' : libraries[-1]['LibraryVersion']}

def submit_job(job_name: str, pyspark_script: str,
               script_parameters: "List[script_parameter]" = None,
               import_tables: "List[import_table]" = None,
               export_tables: "List[export_table]" = None,
               dependencies: "List[library]" = None,
               workspace_id: str = None, cluster_id: str = None,
               run_retry: bool = False, max_concurrent_runs: int = None):
    """
    Submit a spark job (template) and recieve back the JobId
    """
    return neuro_call("80", "sparkmanager", "submitjob", 
                                     {
                                       "JobName" : job_name,
                                       "Script" : pyspark_script,
                                       "ScriptLanguage" : 0,
                                       "ScriptParameters" : script_parameters,
                                       "ImportTables" : import_tables,
                                       "ExportTables" : export_tables,
                                       "WorkspaceId" : workspace_id,
                                       "ClusterId" : cluster_id,
                                       "RunRetry" : run_retry,
                                       "MaxConcurrentRuns" : max_concurrent_runs,
                                       "LibraryDependencies" : dependencies
                                     }
                                    )

def remove_job(job_id: str):
    """
    Remove a spark manager job
    """
    neuro_call("80", "sparkmanager", "removejob", {"JobId":job_id})

def list_jobs(workspace_id: str = None, cluster_id: str = None, max_returned: int = None):
    """
    List the jobs submitted to spark manager
    """
    list_jobs_response = neuro_call("80", "sparkmanager", "listjobs", 
                                     {
                                         "WorkspaceId" : workspace_id,
                                         "ClusterId" : cluster_id,
                                         "NumberReturned" : max_returned
                                     }
                                   )
    return list_jobs_response["JobSummaries"]

def get_job_details(job_id: str):
    """
    Get details about a submitted job
    """
    get_job_details_response = neuro_call("80", "sparkmanager", "getjobdetails", 
                                     {
                                         "JobId":job_id
                                     }
                                   )
    return get_job_details_response["JobDetails"]

def run_job(job_id: str, run_name: str, 
            override_script_parameters: "List[script_parameter]" = None,
            override_import_tables: "List[import_table]" = None,
            override_export_tables: "List[export_table]" = None):
    """
    Run an instance of a submitted job.
    """
    return neuro_call("80", "sparkmanager", "runjob", 
                      {
                          "JobId" :  job_id,
                          "RunName" : run_name,
                          "OverrideScriptParameters" : override_script_parameters,
                          "OverrideImportTables" : override_import_tables,
                          "OverrideExportTables" : override_export_tables
                      }
                     )

def list_runs(job_id: str, schedule_id: str = None, submitted_by: str = None, max_returned: int = None, run_id: str = None):
    """
    List runs for a spark manager job
    """
    list_runs_response = neuro_call("80", "sparkmanager", "listruns", 
                                     {
                                         "JobId" : job_id,
                                         "ScheduleId" : schedule_id,
                                         "SubmittedBy" : submitted_by,
                                         "NumberReturned" : max_returned,
                                         "RunId" : run_id
                                     }
                                   )
    return list_runs_response["RunSummaries"]

def get_job_id(name_substr: str, workspace_id: str = None, cluster_id: str = None) -> str:
    """
    Get a job ID either by index in list_jobs() or JobName.
    """
    job_list = list_jobs(workspace_id=workspace_id, cluster_id=cluster_id)
    if isinstance(name_substr, str):
        job_candidates = [job for job in job_list if name_substr in job['JobName']]
        if len(job_candidates)==1:
            jobID = next((job['JobId'] for job in job_candidates if 'JobId' in job.keys()), None)
            if jobID is None:
                raise ProcessLookupError(f'Output of list_jobs() did not contain JobId key, please reach out to support.')
            else:
                return jobID
        elif len(job_candidates)>1:
            job_candidates_multi = [job['JobName'] for job in job_candidates]
            raise ValueError(
                f'Multiple jobs found with given substring {name_substr}: {job_candidates_multi}.\
                    Please call again with more specific substring.'
                )
        else:
            raise ValueError(f'No job found with given {name_substr} substring, please confirm job name.')
    else:
        raise TypeError(f'The look up arg `name_substr` must be a string, got {type(name_substr)}')

def list_jobs_by_name(look_up: str,
                      return_dict: bool = False,
                      regex: bool = False,
                      regex_flag: re.RegexFlag = 0,
                      get_schedules: bool = False,
                      get_runs: bool = False,
                      workspace_id: str = None,
                      cluster_id: str = None,
                      jobs_max_returned: int = None,
                      runs_max_returned: int = None,
                      runs_submitted_by: str = None,
                     ) -> dict:
    """
    Find the jobs submitted to spark manager, by a either regex or substring lookup.
    If return_dict True, then dict is returned with JobId as key, otherwise results are printed. 
    Additionally you can request additional information about schedules and/or runs associated with the jobs.
    NOTE: max_returned applies to both runs and jobs.
    """
    # define generic text wrappers and return_dict
    return_output = {} if return_dict else None
    preferredWidth = 150
    subpreferredWidth = 120
    run_prefix = "    Runs: "
    run_wrapper = textwrap.TextWrapper(initial_indent=run_prefix, width=subpreferredWidth,
                               subsequent_indent=' '*len(run_prefix))
    sch_prefix = "    Schedules: "
    sch_wrapper = textwrap.TextWrapper(initial_indent=sch_prefix, width=subpreferredWidth,
                                       subsequent_indent=' '*len(sch_prefix))
    
    # API call for jobs and checking dict keys are still valid
    available_jobs = list_jobs(workspace_id=workspace_id, cluster_id=cluster_id)
    if any([False if "JobName" in job_i.keys() else True for job_i in available_jobs]):
        raise KeyError("JobName does not exist in job return dicts. Please raise with Support.")
    if any([False if "JobId" in job_i.keys() else True for job_i in available_jobs]):
        raise KeyError("JobId does not exist in job return dicts. Please raise with Support.")
    
    # Perform look up, either through regex or naive substring. Truncate list by jobs_max_returned if provided.
    if regex:
        selected_jobs = [job_i for job_i in available_jobs if bool(re.search(look_up, job_i["JobName"], flags=regex_flag))]
    else:
        selected_jobs = [job_i for job_i in available_jobs if look_up in job_i["JobName"]]
    if jobs_max_returned:
        selected_jobs = selected_jobs[:jobs_max_returned]
    
    # Go through each job printing details, and schedules/runs if requested.
    if not return_dict:
        if len(selected_jobs)==0:
            print(f"For look_up {look_up}, no jobs found.")  
        else:
            print(f"For look_up {look_up}, the following jobs were found:")
    for job in selected_jobs:
        job_name = str(job['JobName'])
        job_id = str(job['JobId'])
        if return_dict:
            return_output[job_id] = job
        else:
            prefix = job_name + ": "
            wrapper = textwrap.TextWrapper(initial_indent=prefix, width=preferredWidth,
                                       subsequent_indent=' '*len(prefix))
            print(wrapper.fill(str(job)))
        if get_schedules:
            try:
                job_dets = get_job_details(job_id)
            except:
                print(f"Unable to find job reference for {job_name}")
            if "Schedules" not in job_dets.keys():
                raise KeyError("Schedules key not in get_job_details dicts. Please raise with Support.")
            schedules = job_dets["Schedules"]
            if return_dict:
                return_output[job_id]["Schedules"] = schedules
            else:
                sch_message = "\n".join([str(sch) for sch in schedules])
                print(sch_wrapper.fill(sch_message))
        if get_runs:
            try:
                runs = list_runs(job_id=job_id, submitted_by=runs_submitted_by, max_returned=runs_max_returned)
            except:
                print(f"Unable to find job reference for {job_name}")
            if return_dict:
                return_output[job_id]["Runs"] = runs
            else:
                run_message = "\n".join([str(run) for run in runs]) 
                print(run_wrapper.fill(run_message))
    return return_output

def _run_job_wrapper(job_id: str, run_name: str) -> Tuple[bool, str]:
    """
    Wrapper for run_job, used to run a job and then check the status of the run.
    """
    start_time = datetime.datetime.now(datetime.timezone.utc)
    run_metadata = run_job(job_id=job_id, run_name=run_name)
    print(f"Run {run_name} started on Databricks Backend. Waiting for completion")
    
    if run_metadata['ErrorCode']==0:
        time.sleep(10)
        time_check = True
        time_counter = 1
        while time_check:
            time.sleep(10)
            runs_info = list_runs(job_id)
            func_run_info = next((run_json for run_json in runs_info
                                if run_json["RunId"]==run_metadata['RunId']),
                                None)
            if func_run_info is None:
                return False, "Unable to find RunId in run history."
            run_status = func_run_info['Status'].lower()
            if match_status('running', run_status):
                if check_and_break_while(start_time, max_time=900*time_counter, raise_error=False, process=f"Run {run_name}"):
                    time_counter+=1
                    print(f"Run {run_name} has taken {15*time_counter} minutes and is continuing, if this is unexpected please verify manually.")
            elif match_status('failed', run_status):
                return False, func_run_info['Message']
            elif match_status('finished', run_status):
                print(f"Run {run_name} has completed successfully. Took {(datetime.datetime.now(datetime.timezone.utc)-start_time).seconds/60} minutes.")
                return True, 'Success'
            else:
                raise ValueError(f"Run status {func_run_info['Status']} not in expected subset: Running, Failed, Finished.")
    else:
        return False, run_metadata['Error']

@singledispatch
def run_manual_job(job_name, cluster_id: str = None, workspace_id: str = None, run_name:str = None) -> Tuple[bool, str]:
    """
    Run either one job, or, if a list is provided, a sequence of jobs. This job_name will be a look_up string or substring
    for a job on Neuroverse, this job(s) is found in the job directory and then a run is generated with the run_name provided
    (if run_name==None then a random run_name is generated).
    This function is recursive and will accept nested lists, so long the final non-list elements are strings.
    NOTE: This function should be used with caution as it will trigger live jobs, only use look-up job names which you are confident will
    hit your intended target.
    """
    raise ValueError(f"job_name must be str or list, got {type(job_name)}")

@run_manual_job.register(str)
def _(job_name:str, cluster_id=None, workspace_id=None, run_name=None):
    available_jobs = list_jobs(workspace_id=workspace_id, cluster_id=cluster_id)
    valid_jobs = []
    extact_matchs = []
    if run_name is None:
        run_name_tmp = f"{job_name}_manual_{datetime.datetime.now(datetime.timezone.utc)}"
    else:
        run_name_tmp = run_name
    
    # search all jobs on cluster for those matching or including given job name
    for job_i in available_jobs:
        if job_name==job_i["JobName"]:
            extact_matchs.append(job_i)
            valid_jobs.append(job_i)
        elif job_name in job_i["JobName"]:
            valid_jobs.append(job_i)
        else:
            pass
    
    # check the valid_jobs and begin job if 1 otherwise return warning
    if len(valid_jobs)==0:
        error_msg = f'No jobs found on cluster containing {job_name}, confirm cluster and job list.'
        print(error_msg)
        return False, error_msg
    elif len(valid_jobs)>1:
        if len(extact_matchs)==1:
            print(f'Multiple jobs found with given {job_name} substring, however one job had exact \
                  job name and so will run this job'
                 )
            return _run_job_wrapper(job_id=extact_matchs[0]['JobId'], run_name=run_name_tmp)        
        elif len(extact_matchs)>1:
            error_msg = f'Multiple jobs found with exact job name provided, {job_name}, please review jobs \
                  on cluster and remove duplicate jobs before using this function'
            print(error_msg)
            return False, error_msg
        else:
            error_msg = f'The {job_name} arg had multiple options, please review jobs \
                  and either remove duplicate jobs or use an exact name for job_name arg.'
            print(error_msg)
            return False, error_msg            
    elif len(valid_jobs)==1:
        print(f"Commencing job: {valid_jobs[0]['JobName']}")
        return _run_job_wrapper(job_id=valid_jobs[0]['JobId'], run_name=run_name_tmp)
    else:
        raise IndexError(f'valid_jobs has an unexpected length: {len(valid_jobs)}')

@run_manual_job.register(list)
def _(job_name:list, cluster_id=None, workspace_id=None, run_name=None):
    run_results = []
    for run_request in job_name:
        result_tuple = run_manual_job(run_request,cluster_id,workspace_id,run_name)
        run_results.append(result_tuple)
        
        # Evaluate the result using the True / False inital element to decide if job succeeded, breaking out of
        # for-loop sequence if a job fails. Also checks if result_status is a iterable, meaning the function is returning
        # a sequence in a multi-sequence call, in which case the sequence results are printed to the screen. 
        result_status = next(iter(result_tuple or []), False)
        if isinstance(result_status, (list, tuple)):
            print_statement = [(job_i, status) for job_i, status in zip(job_name, run_results)]
            print("Proceeding to next sequence after the following results:")
            for job_i, status in print_statement:
                print(f"{job_i}: {status}")
        elif not isinstance(result_status, bool) or result_status==False:
            print('One of jobs listed returned Failed/Error or the returning output was corrupted')
            return run_results
        else:
            pass
    return run_results

def run_schedule(job_id: str, schedule_name: str, utc_cron_expression: str, 
            override_script_parameters: "List[script_parameter]" = None,
            override_import_tables: "List[import_table]" = None,
            override_export_tables: "List[export_table]" = None):
    """
    Start a run schedule on a spark manager job. A cron expression is used to specify the schedule based on utc time.
    """
    return neuro_call("80", "sparkmanager", "runschedule", 
                      {
                          "JobId" :  job_id,
                          "ScheduleName" : schedule_name,
                          "UtcCronExpression" : utc_cron_expression,
                          "OverrideScriptParameters" : override_script_parameters,
                          "OverrideImportTables" : override_import_tables,
                          "OverrideExportTables" : override_export_tables
                      }
                     )

def stop_schedule(schedule_id: str):
    """
    Stop a run schedule on a spark manager job
    """
    neuro_call("80", "sparkmanager", "stopschedule", {"ScheduleId":schedule_id})
    
def load_pyspark_notebook_to_str(file_name: str):
    """
    Read a python notebook as a string into a variable. This variable can be given to submit_job
    """
    tmp_file=str(uuid.uuid4())
    if not os.path.isfile(file_name) :
        raise Exception(file_name + " does not exist")
    os.system("jupyter nbconvert --to script '" + file_name +"' --output '" + tmp_file + "'")
    file=open(tmp_file+".py")
    script=file.read()
    file.close()
    os.remove(tmp_file+".py")
    return script
    
def list_libraries(workspace_id: str = None, cluster_id: str = None, show_all: bool = False):
    """
    List the non default libraries available on the cluster
    """
    list_jobs_response = neuro_call("80", "sparkmanager", "ListClusterLibraries", 
                                     {
                                         "WorkspaceId" : workspace_id,
                                         "ClusterId" : cluster_id
                                     }
                                   )
    if show_all:
        return list_jobs_response["Libraries"]
    else:
        tmp_libraries=sorted(list_jobs_response["Libraries"],key=lambda x:str(x['LibraryType'])+x['LibraryName']+x['LibraryVersion'])
        libraries=[]
        for n in range(0,len(tmp_libraries)):
            i=tmp_libraries[n]
            if i['Status']=='INSTALLED' or i['Status']=='PENDING':
                libraries.append(i)
            elif i['Status'] == 'UNINSTALL_ON_RESTART' and len(libraries)>0 and libraries[-1]['LibraryType']==i['LibraryType'] and libraries[-1]['LibraryName']==i['LibraryName']:
                if libraries[-1]['Status']=='INSTALLED':
                    libraries[-1]['Status']='PENDING'
                    libraries.append(i)
                else:
                    libraries[-1]=i
        return libraries

def install_library(library_name: str, library_version: str, library_uri: str = None, library_type: int = 0, workspace_id: str = None, cluster_id: str = None):
    """
    Install non default libraries on the cluster
    """
    list_jobs_response = neuro_call("80", "sparkmanager", "InstallClusterLibrary", 
                                     {
                                         "WorkspaceId" : workspace_id,
                                         "ClusterId" : cluster_id,
                                         "LibraryName" : library_name,
                                         "LibraryVersion" : library_version,
                                         "LibraryType" : library_type,
                                         "LibraryRepositoryUri" : library_uri
                                     }
                                   )
    
def uninstall_library(library_name: str, library_version: str, library_type: int = 0, workspace_id: str = None, cluster_id: str = None):
    """
    Uninstall non default libraries on the cluster
    """
    list_jobs_response = neuro_call("80", "sparkmanager", "UninstallClusterLibrary", 
                                     {
                                         "WorkspaceId" : workspace_id,
                                         "ClusterId" : cluster_id,
                                         "LibraryName" : library_name,
                                         "LibraryVersion" : library_version,
                                         "LibraryType" : library_type
                                     }
                                   )

def upgrade_library(library_name: str, library_version: str, force: bool = False, library_uri: str = None, library_type: int = 0, workspace_id: str = None, cluster_id: str = None):
    """
    Install non default libraries on the cluster
    """
    list_jobs_response = neuro_call("80", "sparkmanager", "UpgradeClusterLibrary", 
                                     {
                                         "WorkspaceId" : workspace_id,
                                         "ClusterId" : cluster_id,
                                         "LibraryName" : library_name,
                                         "LibraryVersion" : library_version,
                                         "LibraryType" : library_type,
                                         "LibraryRepositoryUri" : library_uri,
                                         "ForceJobDependenciesUpdate" : force
                                     }
                                   )
def create_cluster(cluster_name: str, spark_version: str = None, node_type_id: str = None, min_workers: int = None, max_workers: int = None, auto_terminate_in_min: int = None, scheduled_workloads_only: bool = None, default_cluster: bool = None, workspace_id: str = None):
    """
    Create a new spark cluster
    """
    create_cluster_response = neuro_call("80", "sparkmanager", "CreateCluster", 
                                     {
                                         "WorkspaceId" : workspace_id,
                                         "ClusterName" : cluster_name,
                                         "SparkVersion" : spark_version,
                                         "NodeTypeId" : node_type_id,
                                         "MinWorkers" : min_workers,
                                         "MaxWorkers" : max_workers,
                                         "AutoTerminationMinutes" : auto_terminate_in_min,
                                         "ScheduledWorkLoadsOnly" : scheduled_workloads_only,
                                         "DefaultCluster" : default_cluster
                                     }
                                   )
    del create_cluster_response['Error']
    del create_cluster_response['ErrorCode']
    return create_cluster_response['ClusterId']

def edit_cluster(cluster_name: str=None, spark_version: str = None, node_type_id: str = None, min_workers: int = None, max_workers: int = None, auto_terminate_in_min: int = None, scheduled_workloads_only: bool = None, default_cluster: bool = None, cluster_id: str = None, workspace_id: str = None):
    """
    Edit a spark cluster
    This will cause it to restart
    """
    edit_cluster_response = neuro_call("80", "sparkmanager", "EditCluster", 
                                     {
                                         "ClusterId" : cluster_id,
                                         "WorkspaceId" : workspace_id,
                                         "ClusterName" : cluster_name,
                                         "SparkVersion" : spark_version,
                                         "NodeTypeId" : node_type_id,
                                         "MinWorkers" : min_workers,
                                         "MaxWorkers" : max_workers,
                                         "AutoTerminationMinutes" : auto_terminate_in_min,
                                         "ScheduledWorkLoadsOnly" : scheduled_workloads_only,
                                         "DefaultCluster" : default_cluster
                                     }
                                   )

def list_clusters(workspace_id: str = None):
    """
    List spark clusters
    """
    list_clusters_response = neuro_call("80", "sparkmanager", "ListClusters", 
                                     {
                                         "WorkspaceId" : workspace_id
                                     }
                                   )
    del list_clusters_response['Error']
    del list_clusters_response['ErrorCode']
    return list_clusters_response['Clusters']

def list_workspaces():
    """
    List spark workspaces
    """
    list_clusters_response = neuro_call("80", "sparkmanager", "ListWorkspaces", 
                                     {
                                         
                                     }
                                   )
    del list_clusters_response['Error']
    del list_clusters_response['ErrorCode']
    return list_clusters_response['Workspaces']

def get_workspace_id(find: Union[str, int] = 0) -> str:
    """
    Get a workspace ID either by index or WorkspaceName
    """
    if isinstance(find, str):
        workspaceID = next((wrk_json["WorkspaceId"] for wrk_json in list_workspaces()
                            if wrk_json["WorkspaceName"]==find),
                            None
                          )
        if not workspaceID:
            print(f"No Workspace with given name: `{find}`")
        return workspaceID
    elif isinstance(find, int):
        return list_workspaces()[find]["WorkspaceId"]
    else:
        raise TypeError(f'The look up arg `find` must be a string or integer, got {type(find)}')

def get_cluster_id(find: Union[str, int], workspace_id: str = None) -> str:
    """
    Get a cluster ID either by index or full name
    """
    cluster_list = list_clusters(workspace_id)
    if isinstance(find, str):
        clusterID = next((clst_json["ClusterId"]
                          for clst_json in cluster_list
                          if f'"cluster_name":"{find}"' in clst_json["Request"]),
                          None
                        )
        if not clusterID:
            print(f"No Cluster with given name: `{find}`")
        return clusterID
    elif isinstance(find, int):
        if find<0 or find>len(cluster_list)-1:
            raise ValueError(f'Index (beginning at 0) entered is out of range of available clusters: {len(cluster_list)-1}'
                            )
        else:
            return cluster_list[find]["ClusterId"]
    else:
        raise TypeError(f'The look up arg `find` must be a string or integer, got {type(find)}')


def get_cluster_status(cluster_id: str = None, workspace_id: str = None) -> str:
    """
    return cluster status from neuro_call
    """
    return next((cluster['State']
                 for cluster in list_clusters(workspace_id=workspace_id)
                 if cluster['ClusterId']==cluster_id),
                 'NO CLUSTER FOUND'
                )

def restart_cluster(cluster_id: str = None, workspace_id: str = None):
    """
    Restart a cluster
    Useful for downgrading libraries
    """
    restart_cluster_response = neuro_call("80", "sparkmanager", "RestartCluster", 
                                     {
                                         "ClusterId" : cluster_id,
                                         "WorkspaceId" : workspace_id
                                     }
                                   )

def start_cluster(cluster_id: str = None, workspace_id: str = None):
    """
    Start a cluster
    """
    start_cluster_response = neuro_call("80", "sparkmanager", "StartCluster", 
                                     {
                                         "ClusterId" : cluster_id,
                                         "WorkspaceId" : workspace_id
                                     }
                                   )

def kickoff_cluster(cluster_id: str = None, workspace_id: str = None, force_restart: bool = False):
    """
    Allows user to attempt to start cluster safely, not doing so if cluster is running. Will also 
    alert user when spin up is complete.
    """
    start_time = datetime.datetime.now(datetime.timezone.utc)
    status = get_cluster_status(cluster_id, workspace_id)
    if match_status("running", status) and not force_restart:
        print('Cluster is already running.')
    elif match_status("running", status) and force_restart:
        print('Cluster running, forcing restart now. This will take 3 - 10 minutes.')
        restart_cluster(cluster_id, workspace_id)
        time.sleep(10)
        status = get_cluster_status(cluster_id, workspace_id)
        while not match_status("running", status):
            time.sleep(10)
            status = get_cluster_status(cluster_id, workspace_id)
            check_and_break_while(start_time)
        print("Finished restart, Cluster is ready")
    elif match_status(['pending', 'resizing'], status):
        print(f'Cluster is in a {status.lower()} state, will be ready soon.')
        while not match_status("running", status):
            time.sleep(10)
            status = get_cluster_status(cluster_id, workspace_id)
            check_and_break_while(start_time)
        print("Cluster is ready")
    elif match_status("terminate", status):
        print(f'Spinning up cluster now. This will take 3 - 10 minutes.')
        start_cluster(cluster_id, workspace_id)
        time.sleep(5)
        while not match_status("running", status):
            time.sleep(10)
            status = get_cluster_status(cluster_id, workspace_id)
            check_and_break_while(start_time)
        print("Cluster is ready")
    elif match_status("terminating", status):
        print(f'Cluster is currently terminating. Function will wait until this process is complete and then begin again.')
        # waiting for cluster to terminate
        time.sleep(5)
        while not match_status("terminate", status):
            time.sleep(10)
            status = get_cluster_status(cluster_id, workspace_id)
            check_and_break_while(start_time)
        
        # starting cluster
        time.sleep(5)
        start_cluster(cluster_id, workspace_id)
        time.sleep(5)
        while not match_status("running", status):
            time.sleep(10)
            status = get_cluster_status(cluster_id, workspace_id)
            check_and_break_while(start_time)
        print("Cluster is ready")
    else:
        raise ProcessLookupError(f'Cluster was in unexpected state ({status}) please reach out to support')

def delete_cluster(cluster_id: str = None, workspace_id: str = None):
    """
    Delete a cluster
    """
    delete_cluster_response = neuro_call("80", "sparkmanager", "DeleteCluster", 
                                     {
                                         "ClusterId" : cluster_id,
                                         "WorkspaceId" : workspace_id
                                     }
                                   )

def cancel_run(run_id: str):
    """
    Cancel a running instance of a job
    """
    cancel_run_response = neuro_call("80", "sparkmanager", "CancelRun", 
                                     {
                                         "RunId" : run_id
                                     }
                                   )

def create_context(context_name:str, cluster_id: str = None, workspace_id: str = None):
    """
    Create an interactive spark context
    """
    global context_id
    create_context_response = neuro_call("80", "sparkmanager", "CreateContext", 
                                     {
                                         "ClusterId" : cluster_id,
                                         "WorkspaceId" : workspace_id,
                                         "ScriptLanguage" : "Python",
                                         "ContextName" : context_name
                                     }
                                   )
    context_id=create_context_response['ContextId']
    del create_context_response['Error']
    del create_context_response['ErrorCode']
    return create_context_response

def initialize_notebook(cluster_id_find: Union[str, int] = 0, workspace_id_find: Union[str, int] = 0, context_name: str = None, force_restart: bool = False):
    """
    Allows user to make call to spin up / access the cluster and create a context with one function.
    """
    workspaceID = get_workspace_id(workspace_id_find)
    clusterID = get_cluster_id(find=cluster_id_find, workspace_id=workspaceID)
    kickoff_cluster(cluster_id=clusterID, workspace_id=workspaceID, force_restart=force_restart)

    # defining a spark session context
    time.sleep(2)
    if context_name is None:
        context_name = str(uuid.uuid4())[:8]+'-context'
    
    context_text = create_context(context_name=context_name, cluster_id=clusterID, workspace_id=workspaceID)
    print("Context generated")
    return context_text

def inspect_context(context_id: str):
    """
    Inspect status of an interactive spark context
    """
    inspect_context_response = neuro_call("80", "sparkmanager", "InspectContext", 
                                     {
                                         "ContextId" : context_id
                                     }
                                   )
    del inspect_context_response['Error']
    del inspect_context_response['ErrorCode']
    return inspect_context_response

def destroy_context(context_id: str):
    """
    Destroy an interactive spark context
    """
    destroy_context_response = neuro_call("80", "sparkmanager", "DestroyContext", 
                                     {
                                         "ContextId" : context_id
                                     }
                                   )

def list_contexts(cluster_id: str = None, workspace_id: str = None):
    """
    List interactive spark contexts
    """
    list_contexts_response = neuro_call("80", "sparkmanager", "ListContexts", 
                                     {
                                         "ClusterId" : cluster_id,
                                         "WorkspaceId" : workspace_id
                                     }
                                   )
    del list_contexts_response['Error']
    del list_contexts_response['ErrorCode']
    return list_contexts_response

def execute_command(context_id: str, command_name: str, command: str):
    """
    Triggers a command to be executed in a spark context
    """
    execute_command_response = neuro_call("80", "sparkmanager", "ExecuteCommand", 
                                     {
                                         "ContextId" : context_id,
                                         "CommandName" : command_name,
                                         "ScriptLanguage" : "Python",
                                         "Command" : command
                                     }
                                   )
    del execute_command_response['Error']
    del execute_command_response['ErrorCode']
    return execute_command_response

def execute_import_table_command(context_id: str, import_table: "import_table"):
    """
    Triggers a command to import a table into a spark context
    """
    execute_import_table_command_response = neuro_call("80", "sparkmanager", "ExecuteImportTableCommand", 
                                     {
                                         "ContextId" : context_id,
                                         "ScriptLanguage" : "Python",
                                         "ImportTable" : import_table
                                     }
                                   )
    del execute_import_table_command_response['Error']
    del execute_import_table_command_response['ErrorCode']
    return execute_import_table_command_response

def execute_export_table_command(context_id: str, export_table: "export_table"):
    """
    Triggers a command to export a table from a spark context
    """
    execute_export_table_command_response = neuro_call("80", "sparkmanager", "ExecuteExportTableCommand", 
                                     {
                                         "ContextId" : context_id,
                                         "ScriptLanguage" : "Python",
                                         "ExportTable" : export_table
                                     }
                                   )
    del execute_export_table_command_response['Error']
    del execute_export_table_command_response['ErrorCode']
    return execute_export_table_command_response

def cancel_command(command_id: str):
    """
    Cancel a running command in a context
    """
    cancel_command_response = neuro_call("80", "sparkmanager", "CancelCommand", 
                                     {
                                         "CommandId" : command_id
                                     }
                                   )

def list_commands(context_id: str):
    """
    List commands in a context
    """
    list_commands_response = neuro_call("80", "sparkmanager", "ListCommands", 
                                     {
                                         "ContextId" : context_id
                                     }
                                   )
    del list_commands_response['Error']
    del list_commands_response['ErrorCode']
    return list_commands_response

def inspect_command(command_id: str):
    """
    Inspect the status and result of a command
    """
    inspect_command_response = neuro_call("80", "sparkmanager", "InspectCommand", 
                                     {
                                         "CommandId" : command_id
                                     }
                                   )
    del inspect_command_response['Error']
    del inspect_command_response['ErrorCode']
    return inspect_command_response

def spark_magic(button,progress,command,out,output,user_ns,silent=False):
    stop=[0,0]
    
    def on_button_clicked(b):
        stop[0]=1
    button.on_click(on_button_clicked)
    def work(command,stop,out,output,user_ns,silent):
        while inspect_command(command['CommandId'])['Status']!='Finished' and stop[0]==0:
            time.sleep(1)
        if stop[0]==1:
            cancel_command(command['CommandId'])
            stop[0]=1
            output.append_display_data('Cancelled')
        else:
            result=inspect_command(command['CommandId'])
            if result['Result']['ResultType']=='error':
                stop[0]=1
                output.append_display_data(HTML(result['Result']['Summary']))
            else:
                if out is None:
                    stop[0]=1
                    stop[1]=1
                    if not silent:
                        if result['Result']['Data']!='':
                            output.append_display_data(result['Result']['Data'])
                else:
                    stop[0]=1
                    stop[1]=1
                    user_ns[out] = result['Result']['Data']

    
    thread = threading.Thread(target=work, args=(command,stop,out,output,user_ns,silent))
    thread.start()

    

    def progress_work(progress,stop):
        while stop[0]==0:
            progress.value = 0
            total = 100
            for i in range(total):
                time.sleep(0.02)
                progress.value = float(i+1)/total
                if stop[0]==1:
                    break
        progress.value = 1

    thread1 = threading.Thread(target=progress_work, args=(progress,stop,))
    thread1.start()
    return stop

def checkForLimits(dataframe_name,store_name):
    if store_name=='NeuroverseEvents':
        findLimitCommand='''
if 'LocalLimit' in %s._jdf.queryExecution().toString():
    print(True)
else:
    print(False)'''%dataframe_name
        command=execute_command(context_id,'findLimit',findLimitCommand)
        while inspect_command(command['CommandId'])['Status']!='Finished':
            time.sleep(1)
        result=inspect_command(command['CommandId'])
        if result['Result']['Data']=='True':
            print("Your limit on DataFrame:%s will not be applied. Consider cancelling and using a where clause instead."%dataframe_name)  

@magics_class
class SparkMagics(Magics):
    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--out', '-o',
      help='The variable to return the results in'
    )
    def spark(self, line, cell):
        global context_id
        args = magic_arguments.parse_argstring(self.spark, line)
        contextid='"%s"'%context_id
        out=args.out
        command=execute_command(eval(contextid),'1',cell)
        
        output = widgets.Output()
        button = widgets.Button(description="Cancel")
        progress = widgets.FloatProgress(value=0.0, min=0.0, max=1.0)
        display(widgets.VBox([output,widgets.HBox([widgets.Label("CommandId: %s"%command['CommandId']),progress,button])]))
        
        spark_magic(button,progress,command,out,output,self.shell.user_ns)
    
    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--dataframe', '-df',
      help='DataFrame to be assigned into'
    )
    @magic_arguments.argument('--out', '-o',
      help='The variable to return the results in'
    )
    def spark_sql(self, line, cell):
        global context_id
        contextid=None
        args = magic_arguments.parse_argstring(self.spark_sql, line)
        contextid='"%s"'%context_id
        out=args.out
        if args.dataframe!=None:
            dataframe=args.dataframe
            code = ('%s=spark.sql("%s")\n%s.createOrReplaceTempView("%s")'%(dataframe,cell.replace('\n',' '),dataframe,dataframe))
        else:
            dataframe="df%s"%(str(uuid.uuid4()).replace('-','_'))
            code = ('%s=spark.sql("%s")'%(dataframe,cell.replace('\n',' ')))
        
        command=execute_command(eval(contextid),'1',code)
        
        output = widgets.Output()
        button = widgets.Button(description="Cancel")
        progress = widgets.FloatProgress(value=0.0, min=0.0, max=1.0)
        display(widgets.VBox([output,widgets.HBox([widgets.Label("CommandId: %s"%command['CommandId']),progress,button])]))
        
        stop=spark_magic(button,progress,command,out,output,self.shell.user_ns,silent=True)
        while stop[0]==0:
            time.sleep(1)
        if stop[1]==1:
            if args.out!=None or args.dataframe==None:
                command=execute_command(eval(contextid),'1','str(%s.columns)'%dataframe)
                schema_out='A'+str(uuid.uuid4())
                stop=spark_magic(button,progress,command,schema_out,output,self.shell.user_ns,silent=True)
                while stop[0]==0:
                    time.sleep(1)
                if stop[1]==1:
                    columns=[]
                    for col in self.shell.user_ns[schema_out].split('[')[-1].strip(']"').replace("'","").split(','):
                        columns.append(col)
                    command2=execute_command(eval(contextid),'1','display(%s)'%dataframe)
                    data_out='A'+str(uuid.uuid4())
                    stop=spark_magic(button,progress,command2,data_out,output,self.shell.user_ns,silent=True)
                    while stop[0]==0:
                        time.sleep(1)
                    if stop[1]==1:
                        if args.out is None:
                            output.append_display_data(HTML(pd.DataFrame.from_records(self.shell.user_ns[data_out],columns=columns).to_html()))
                        else:
                            self.shell.user_ns[args.out] = pd.DataFrame.from_records(self.shell.user_ns[data_out],columns=columns)
    @line_magic
    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--dataframe', '-df',
      help='The dataframe name to store the table in'
    )
    @magic_arguments.argument('--storename', '-sn',
      help='The data store name'
    )
    @magic_arguments.argument('--tablename', '-tn',
      help='The table name'
    )
    @magic_arguments.argument('--partitionpaths', '-pp',
      help='The partition paths'
    )
    @magic_arguments.argument('--sqlquery', '-sq',
      help='The sql query'
    )
    def spark_import_table(self, line, cell=None):
        global context_id
        args = magic_arguments.parse_argstring(self.spark_import_table, line)
        contextid='"%s"'%context_id
        output = widgets.Output()
        button = widgets.Button(description="Cancel")
        progress = widgets.FloatProgress(value=0.0, min=0.0, max=1.0)
        display(widgets.VBox([output,widgets.HBox([widgets.Label("Command: Import tables"),progress,button])]))
        
        if args.dataframe!=None:
            temp_import_table=import_table(args.dataframe,args.storename,args.tablename,
                                           args.partitionpaths or ["'/'"],args.sqlquery)
            command=execute_import_table_command(eval(contextid),temp_import_table)
            stop=spark_magic(button,progress,command,None,output,self.shell.user_ns,silent=True)
            while stop[0]==0:
                time.sleep(1)

            if stop[1]==1:
                code="%s.createOrReplaceTempView('%s')"%(temp_import_table['SparkDataFrameName'],temp_import_table['SparkDataFrameName'])
                command1=execute_command(eval(contextid),'1',code)
                stop=spark_magic(button,progress,command1,None,output,self.shell.user_ns)
        else:
            for cell_line in cell.split('\n'):
                if cell_line != "":
                    command=execute_import_table_command(eval(contextid),eval(cell_line))

                    stop=spark_magic(button,progress,command,None,output,self.shell.user_ns,silent=True)
                    while stop[0]==0:
                        time.sleep(1)

                    if stop[1]==1:
                        temp_import_table=eval(cell_line)
                        code="%s.createOrReplaceTempView('%s')"%(temp_import_table['SparkDataFrameName'],temp_import_table['SparkDataFrameName'])
                        command1=execute_command(eval(contextid),'1',code)
                        stop=spark_magic(button,progress,command1,None,output,self.shell.user_ns)  
    
    @line_magic
    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--dataframe', '-df',
      help='The dataframe name to store the table in'
    )
    @magic_arguments.argument('--storename', '-sn',
      help='The data store name'
    )
    @magic_arguments.argument('--tablename', '-tn',
      help='The table name'
    )
    @magic_arguments.argument('--partitionpath', '-pp',
      help='The partition path'
    )
    @magic_arguments.argument('--writemode', '-wm',
      help='The write mode, either append or overwrite. Defaults to append.')
    def spark_export_table(self, line, cell=None):
        global context_id
        args = magic_arguments.parse_argstring(self.spark_export_table, line)
        contextid='"%s"'%context_id
        output = widgets.Output()
        button = widgets.Button(description="Cancel")
        progress = widgets.FloatProgress(value=0.0, min=0.0, max=1.0)
        display(widgets.VBox([output,widgets.HBox([widgets.Label("Command: Export tables"),progress,button])]))
        if args.dataframe!=None:
            temp_export_table=export_table(args.dataframe,args.storename,args.tablename,
                                           args.partitionpath or "'/'", args.writemode or "append")
            checkForLimits(temp_export_table["SparkDataFrameName"],temp_export_table["DataStoreName"])
            command=execute_export_table_command(eval(contextid),temp_export_table)
            spark_magic(button,progress,command,None,output,self.shell.user_ns)
        else:
            for cell_line in cell.split('\n'):
                if cell_line != "":
                    temp_export_table=eval(cell_line)
                    checkForLimits(temp_export_table["SparkDataFrameName"],temp_export_table["DataStoreName"])
                    command=execute_export_table_command(eval(contextid),temp_export_table)
                    spark_magic(button,progress,command,None,output,self.shell.user_ns)
        
    @line_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--dataframe', '-df',
      help='DataFrame to be printed'
    )
    @magic_arguments.argument('--out', '-o',
      help='The variable to return the results in'
    )
    def spark_pandas(self, line):
        global context_id
        contextid='"%s"'%context_id
        dataframe=''
        args = magic_arguments.parse_argstring(self.spark_pandas, line)
        out=args.out
        if args.dataframe!=None:
            dataframe=args.dataframe
        else:
            raise Exception('dataframe parameter must be provided')
        command=execute_command(eval(contextid),'1','str(%s.columns)'%dataframe)
        output = widgets.Output()
        button = widgets.Button(description="Cancel")
        progress = widgets.FloatProgress(value=0.0, min=0.0, max=1.0)
        display(widgets.VBox([output,widgets.HBox([widgets.Label("CommandId: %s"%command['CommandId']),progress,button])]))
        
        schema_out='A'+str(uuid.uuid4())
        stop=spark_magic(button,progress,command,schema_out,output,self.shell.user_ns,silent=True)
        
        while stop[0]==0:
            time.sleep(1)
        
        if stop[1]==1:
            columns=[]
            for col in self.shell.user_ns[schema_out].split('[')[-1].strip(']"').replace("'","").split(','):
                columns.append(col)

            command2=execute_command(eval(contextid),'1','display(%s)'%dataframe)
            data_out='A'+str(uuid.uuid4())
            stop=spark_magic(button,progress,command2,data_out,output,self.shell.user_ns,silent=True)
            while stop[0]==0:
                time.sleep(1)
        
            if stop[1]==1:
                if args.out is None:
                    output.append_display_data(HTML(pd.DataFrame.from_records(self.shell.user_ns[data_out],columns=columns).to_html()))
                else:
                    self.shell.user_ns[args.out] = pd.DataFrame.from_records(self.shell.user_ns[data_out],columns=columns)
        
ip = get_ipython()
ip.register_magics(SparkMagics)
