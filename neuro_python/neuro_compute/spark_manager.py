"""
The spark_manager module provides functions to interact with the
Spark Manager in Neuroverse.
"""

from neuro_python.neuro_call import neuro_call
import uuid
import os
import datetime
import pandas as pd
pd.set_option('display.max_rows', 1000)
import time

from IPython.core import magic_arguments
from IPython.core.magic import line_magic, cell_magic, line_cell_magic, Magics, magics_class

#Default context id
context_id=''

def script_parameter(name: str, value):
    """
    Cmd line parameter value to be used in the pyspark script. eg sys.argv[1]
    """
    return {"Name":name,"Value":value}
  
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
  
def export_table(dataframe_name: str, data_store_name: str, table_name: str, partition_path: str = "'/'"):
    """
    pyspark script data to be outputed into a Neuroverse datalake.
    partition_path contains string that can be feed into the python eval function and return a str.
    This allows for the partition that is used in a spark job run to be determined at runtime (eg. using the current datetime).
    If you want to use a constant partition path string must be wrapped in quotes. eg "'/2018/1'"
    """
    if partition_path!=None:
        if not isinstance(eval(partition_path), str):
            raise Exception("A string is not returned when evaluating: " + partition_path)
    return {"SparkDataFrameName":dataframe_name, "DataStoreName":data_store_name, "TableName":table_name, "PartitionPath":partition_path}

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
    context_id=create_context_response['ClusterId']
    del create_context_response['Error']
    del create_context_response['ErrorCode']
    return create_context_response

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

@magics_class
class SparkMagics(Magics):
    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--contextid', '-c',
      help='The id of the context to execute the command in'
    )
    def spark_display(self, line, cell):
        global context_id
        args = magic_arguments.parse_argstring(self.spark_display, line)
        if args.contextid is None:
            args.contextid=context_id
        command=execute_command(eval(args.contextid),'1',cell)
        while inspect_command(command['CommandId'])['Status']!='Finished':
            time.sleep(1)
        result=inspect_command(command['CommandId'])
        if result['Result']['ResultType']!='table':
            return result
        return pd.DataFrame.from_records(result['Result']['Data'])


    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--contextid', '-c',
      help='The id of the context to execute the command in'
    )
    @magic_arguments.argument('--out', '-o',
      help='The variable to return the results in'
    )
    def spark(self, line, cell):
        global context_id
        args = magic_arguments.parse_argstring(self.spark, line)
        if args.contextid is None:
            args.contextid=context_id
        command=execute_command(eval(args.contextid),'1',cell)
        while inspect_command(command['CommandId'])['Status']!='Finished':
            time.sleep(1)
        result=inspect_command(command['CommandId'])
        
        if result['Result']['ResultType']=='error':
            return result
        if args.out is None:
            print(result['Result']['Data'])
        else:
            self.shell.user_ns[args.out] = result['Result']['Data']
            
    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--contextid', '-c',
      help='The id of the context to execute the command in'
    )
    def spark_import_table(self, line, cell):
        global context_id
        args = magic_arguments.parse_argstring(self.spark_display, line)
        if args.contextid is None:
            args.contextid=context_id
        command=execute_import_table_command(eval(args.contextid),eval(cell))
        while inspect_command(command['CommandId'])['Status']!='Finished':
            time.sleep(1)
        result=inspect_command(command['CommandId'])
        if result['Result']['ResultType']=='error':
            return result
        print(result['Result']['Data'])
    
    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--contextid', '-c',
      help='The id of the context to execute the command in'
    )
    def spark_export_table(self, line, cell):
        global context_id
        args = magic_arguments.parse_argstring(self.spark_display, line)
        if args.contextid is None:
            args.contextid=context_id
        command=execute_export_table_command(eval(args.contextid),eval(cell))
        while inspect_command(command['CommandId'])['Status']!='Finished':
            time.sleep(1)
        result=inspect_command(command['CommandId'])
        if result['Result']['ResultType']=='error':
            return result
        print(result['Result']['Data'])

ip = get_ipython()
ip.register_magics(SparkMagics)
