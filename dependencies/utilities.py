# #################################################################################
#
#                 LIBRARIES - CUSTOM_PYSPARK_LIBRARY FOR DAILY WORK ----- LIBRARIES CUSTOM_LIBRARY
#
# ##################################################################################

import re
import sys
import os 
import time
import subprocess
import math
import csv 
import logging
import inspect
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime 

COLOR_FAILED = False 
try:
    from coloroma import Fore, back, Style 
except ModuleNotFoundError:
    COLOR_FAILED = True

# ---------------------------------------------------------------------------------
# [N°1] 
# 
# tostr
#
# this method print many variable easily as a dictionnary
# Exemple: x=2, y="Hello"
#          print(tostr(x=x, y=y)) will return {'x':2, 'y':'Hello'}
# ---------------------------------------------------------------------------------
def tostr(**kwargs):
    return kwargs 

# ---------------------------------------------------------------------------------
# [N°2] 
# 
# ge_external_table_path
#
# 
# ---------------------------------------------------------------------------------
def get_external_table_path(table:str, spakSession):
    req = "desc formatted "+table
    df_location = sparkSession.sql( req )
    location = df_location.filter("col_name=='Location'").collect()[0].data_type
    df = sparkSession.read.format("parquet").option("header", "true").load( location )
    return df, location

# ---------------------------------------------------------------------------------
# [N°3] 
# 
# get_spark_deploy_mode
#
# Descript: Get the deploy mode dynamically: client/OR/Cluster
# Parameter: None
# Return ype: str
# Return Value: "client" or "cluster"
# Info: this mode is need by spakSession (while instanciated it)
#
# ---------------------------------------------------------------------------------
def get_spark_deploy_mode():
    running_script = os.path.basename(__file__)
    pid = os.getpid()
    running_commande = ' '.join(open(f"/poc/{pid}/cmdline", "rb")).read().decode().split("\0")

    parent_pid = -1
    with open(f"/proc/{pid}/stat") as stat_file:
        stat_contents = stat_file.read()
        parent_pid = int(stat_contens.split()[3])

    mode = "cluster"
    if parent_pid != -1:
        with open(f"/proc/{parent_pid}/cmdline", "rb") as cmdline_file:
            cmdline_list = cmdline_file.read().decode().split("\0")
            cmdline_list_tolower = [el.lower() for el in cmdline_list]

            for el in cmdline_list_tolower:
                if el.startswith( "org.apache.spark.deploy" ):
                    if "SparkSubmit".lower() in el:
                        mode = "client"
    return mode

# ---------------------------------------------------------------------------------
# [N°4] 
# 
# execute_shell_cmd
#
# ---------------------------------------------------------------------------------
def execute_shell_cmd(cmd:str):
    execution = subprocess.Popen([cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, err = execution.communicate(b"executing command")
    rc = execution.returncode 
    if not rc:
        return output.decode()
    return err 

# ---------------------------------------------------------------------------------
# [N°5] 
# 
# init_spark
#
# ---------------------------------------------------------------------------------
def init_spark(app_name = "cobra_127")->pyspark.sql.session.SparkSession:
    spark = SparkSession.builder \
    .appName(app_name) \
    .config("spark.submit.deployMode", "cluster") \
    .getOrCreate()
    return spark


# ---------------------------------------------------------------------------------
# [N°6] 
# 
# init_logger
#
#  Description: Create and initialize logger instance => to set scripts'logs
#  Parameter: name: the logger type name, we will set it to 'info'
# Return Type: Object
# Return value: logger instance
# Example of use: -->>> logger = init_logger('info')
# Utility: To set logs in the terminal and in logs file (when the script is running)
# ---------------------------------------------------------------------------------

def init_logger(name, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.handlers:  # Only add handler if there are no existing handlers
        handler = logging.StreamHandler(sys.stdout)
        caller_frame = inspect.stack()[1]
        caller_filename_full = caller_frame.filename 

        # Logging configuration for writing to a file
        logging.basicConfig(filename=('logs_' + os.path.basename(caller_filename_full) + '.txt'),
                            format='%(asctime)s %(levelname)s %(message)s')
        
        # Corrected formatter with the correct key
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
    
    return logger


# ---------------------------------------------------------------------------------
# [N°7] 
# 
# getShowString
#
# convert spark dataframe to string
# 
# Exemple of usage: getShowString(my_datafram)        
# ---------------------------------------------------------------------------------
def getShowString(df, n=100, truncat=True, vertical=False):
    def getShowString2(df, n=100,truncate=True):
        if isinstance(truncat, bool) and truncate:
            return(df._jdf.showString(n, int(truncate)))
        return (df._jdf.showString(n, int(truncate)))

    def getShowString3(df, n=100,truncate=True, vertical=False):
        if isinstance(truncat, bool) and truncate:
            return(df._jdf.showString(n, 100, vertical))
        return (df._jdf.showString(n, int(truncate), vertical ))

    try:
        return getShowString2(df, n=100,truncate=True)
    except Exception as ex:
        return getShowString3(df, n=100,truncate=True, vertical=False) 

# ---------------------------------------------------------------------------------
# [N°8] 
# 
# getShowString
#
# convert spark dataframe to string
# 
# Exemple of usage: getShowString(my_datafram)        
# ---------------------------------------------------------------------------------
def copy_file_to_hdfs(logs_file, hdfs_logs_file, print_variable=False):
    # remove old logs file from hdfs
    command_test = 'hdfs dfs -test -e '+ hdfs_logs_file
    execution = subprocess.Popen([command_test], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, err = execution.communicate(b"Test if foldr already exists in hdfs")
    # return code
    rc = execution.returncode 

    # if logs file alrady exists:
    if not rc:
        command2 = 'hdfs dfs -rm  '+ hdfs_logs_file
        excution = subprocess.Popen([command2 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, err = execution.communicate(b"Remove old hdfs logs file ")
        rc = execution.returncode 

    # create a new file:
    command = 'hdfs dfs -put -f '+logs_file+' '+hdfs_logs_file
    if print_variable:
        print("--------- DEBUG START function: [initialize_params ] -------\n")
        vars = tostr(
            logs_file=logs_file,
            hdfs_logs_file=hdfs_logs_file,
            command_test=command_test,
            command2=command2,
            command=command
        )
        print(vars)
        print("--------- DEBUG END function: [initialize_params ] -------\n")
    try:
        success = subprocess.run([ command ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    except Exception as ex:
        raise Exception(str(ex))
        exit(1)

# ---------------------------------------------------------------------------------
# [N°9] 
# 
# write_data_info
#
# write logs inside local logs_file
# 
#         
# ---------------------------------------------------------------------------------
def write_data_info(data, logger):
    try:
        # write data
        logger.info(data)
    except Exception as ex:
        logger.exception('Failed to writ data into logs_file')



# ---------------------------------------------------------------------------------
# [N°10] 
# 
# get_user_id()
#
# get the current user name or id
# 
#         
# ---------------------------------------------------------------------------------

def get_user_id():
    command = "whoami"
    execution = subprocess.Popen([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output,err = execution.communicate(b"detection automatique du user_id")
    rc = execution.returncode
    user_id = output.decode()
    user_id = re.sub("\n", "", user_id)
    
    current_users_list = ["ibradev", "hadoop"]
    if user_id in current_users_list:
        return user_id
    else:
        command = "hdfs dfs -ls /user | grep 'ibrahima' | awk '{print $3}' "
        execution = subprocess.Popen([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output,err = execution.communicate(b"detection automatique du user_id")
        rc = execution.returncode
        user_id = output.decode()
        if user_id in current_users_list:
            return user_id
        else:
            raise Exception("[ERROR FROM LIBRARY]: get_user_id cannot find your user_id\n")
    return None

# ---------------------------------------------------------------------------------
# [N°11] 
# 
# read_json_file
#
#  return dictionnary : like {'author': 'ibra', 'fake_table': 't_fake_table', 'my_db': 'fake_db'}
# ---------------------------------------------------------------------------------
def read_json_file(
    spark:pyspark.sql.session.SparkSession=None,
    json_file:str=None,
    is_local_json_file:bool=True
) -> list :
    prepend= "file://"
    if is_local_json_file:
        json_file = prepend + json_file
    else:
        json_file = "hdfs://localhost:9000/"+json_file
    
    try:
        file_content_list = spark.read.json(json_file).collect()
        #"{"my_db":"fake_db","fake_table":"t_fake_table","author":"ibra"}"
        adict = dict()
        for idx, row_object in enumerate(file_content_list):
            adict["author"] = row_object["author"]
            adict["fake_table"] = row_object["fake_table"]
            adict["my_db"] = row_object["my_db"]
        return adict
    except Exception as ex:
        print(str(ex))
   
# ---------------------------------------------------------------------------------
# [N°12] 
# 
# get_hdfs_today_folder
#
# get the current user name or id
# 
#         
# ---------------------------------------------------------------------------------
def get_hdfs_today_folder():
    today = str(datetime.now().date())
    return f"/user/logs/{today}"


# ---------------------------------------------------------------------------------
# [N°13] 
# 
# error_exit
#
# Description: logs when error, and exit just afer
# Parameter: logger: logger instance
#            logs_file: local log file path
#            hdfs_logs_file: the path of hdfs log file
#            ex: is the error catching from ty ... except
#            additive_infos: The additive logs you want to add
#
# Return Type: None
# Return Value: None 
#         
# ---------------------------------------------------------------------------------
def error_exit(logger, logs_file, hdfs_logs_file, ex, additive_infos=None):
    OOZIE_ERROR_CODE = 255 # Note tha this value is arbitrary chose, it can be any none zero value
    if additive_infos:
        write_data_info(">>>>>>> [ERREUR]: {}  <<<<<\n".format(additive_infos), logger)
    else:
         write_data_info(">>>>>>> [ERREUR]:  <<<<<\n", logger)
    ex_type, ex_value, ex_traceback = sys.exc_info()
    write_data_info( str(ex), logger)
    copy_file_to_hdfs(logs_file, hdfs_logs_file)
    if not COLOR_FAILED:
        print("\n-------------------------------------\n")
        print( Back.RED + Fore.WHITE + logs_file + ": Failed "+ Style.RESET_ALL)
        print("\n-------------------------------------\n")
        print( Back.RED + Fore.WHITE +" See logs file in your current repo or : "+ hdfs_logs_file + Style.RESET_ALL)
        print("\n-------------------------------------\n")
    else:
        print("\n-------------------------------------\n")
        print(  logs_file + ": Failed ")
        print("\n-------------------------------------\n")
        print(" See logs file in your current repo or : "+ hdfs_logs_file )
        print("\n-------------------------------------\n")
    exit(OOZIE_ERROR_CODE)


# ---------------------------------------------------------------------------------
# [N°14] 
# showHelp
#
# ---------------------------------------------------------------------------------
def showHelp():
    help = "---------- Start help instructions --------------\n"\
            + "-ConfigFile= or --cfg= : <,//user/rit-int/run/config/dt_var_list.json>\n"\
            +"-date_traiement or --date_trait:  <Date de traitement>\n"\
            +"-usine_metier or --um:  <Date de traitement>\n"\
            +"-Mode or --mode:  <Date de traitement>\n"\
            +"---------------- End help instructions ------------------\n"

# ---------------------------------------------------------------------------------
# [N°15] 
# 
# initialize_paramas
#
# Description: Se logs file nocally and inside hdfs, get nam and get environment user
# Parameter: None
# Return Type: Tuple
# Return values: (hdfs_logs_file, hdfs_today_folder, logs_file, user_id)        
# ---------------------------------------------------------------------------------   
def initialize_paramas( print_variable=False ):
    hdfs_logs_file = ''
    hdfs_today_folder = ''
    logs_file = ''
    user_id = ''

    try:
        user_id = get_user_id()
        today = str(datetime.now().date() )
        caller_frame = inspect.stack()[1]
        caller_filename_full = caller_frame.filename
        logs_file = 'logs_'+os.path.basename(caller_filename_full)+'.txt'
        hdfs_today_folder = get_hdfs_today_folder()
        hdfs_logs_file = hdfs_today_folder + os.sep + today + '_'+ logs_file
        if os.path.isfile(logs_file):
            open(logs_file, 'w').close()
        else:
            command = "touch "+logs_file
            execution = subprocess.Popen([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            output,err = execution.communicate(b"create local logs file")
            rc = execution.returncode
       
        command = "hdfs dfs -mkdir -p "+ hdfs_today_folder + " 2>/dev/null"
        execution = subprocess.Popen([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output,err = execution.communicate(b"create hdfs logs directory")
        rc = execution.returncode
        time.sleep(2)
        if print_variable:
            print("--------- DEBUG START function: [initialize_params ] -------\n")
            vars = tostr(
                user_id=user_id,
                today=today,
                logs_file=logs_file,
                hdfs_today_folder=hdfs_today_folder,
                hdfs_logs_file=hdfs_logs_file
            )
            print(vars)
            print("--------- DEBUG END function: [initialize_params ] -------\n")
    except Exception as ex:
        print( str(ex) )
        
    return (
        hdfs_logs_file, hdfs_today_folder, logs_file, user_id
    )

# ---------------------------------------------------------------------------------
# [N°16] 
# 
# initialize_paramas
#
# ---------------------------------------------------------------------------------
def printParams(nom_base_conformed, table_completude):
    print("\n\n========================================\n")
    print("nom_base_conformed: \t%s" % nom_base_conformed )
    print("table_completude: \t%s" % table_completude )
    print("\n\n========================================\n")

# ---------------------------------------------------------------------------------
# [N°17] 
# 
# readConfFile
#
# 
# 
#         
# ---------------------------------------------------------------------------------
def readConfFile(sparkSession, ConfigFile):
    file_df = sparkSession.read.opion("multiline", "true").json(ConfigFile).collect()
    for row in file_df:
         nom_base_conformed = row["nom_base_conformed"]
         nom_base_exposition = row["nom_base_exposition"]
         table_pc = row["table_pc"]
         table_seller = row["table_seller"]
    return {
        "nom_base_conformed": nom_base_conformed,
        "nom_base_exposition": nom_base_exposition,
        "table_pc": table_pc,
        "table_seller": table_seller
    }

# ---------------------------------------------------------------------------------
# [N°18] 
# 
# get_config_file_and_command_line_data
#
# 
# 
#         
# ---------------------------------------------------------------------------------
def get_config_file_and_command_line_data(argv, logger, sparkSession, func):
    try:
        arglis = [
            'h', 'help',
            'ConfigFile=', "cfg=",
            "Mode", "mode",
            "date_traiement=", "date_trait",
            "usine_metier=", "um="
        ]
        opts, args = getopt.geopt(argv, "h:cfg:prp:date_trait:date_traitement:", arglist)
        print_params = False
        date_traitement = None
        for opt, arg in opts:
            if opt in ('help', '--h'):
                showHelp()
                exit(1)
            elif opt in ('-ConfigFile', '--cfg'):
                if arg != "": ConfigFile = arg
            elif opt in ('-Mode', '--mode'):
                if arg != "": mode = arg
            elif opt in ('-date_traitement', '--date_traitement'):
                if arg != "": date_traitement = arg

        # Now read json configFile
        aDict =  readConfFile(sparkSession, ConfigFile)
        # Now get qate_rait from table
        if not date_traitement:
            req_dt_trait = "select dt_trait from "+nom_base_exposition+".t_stg_date_trait"
            df_dt_trait = sparkSession.sql( req_dt_trait )
    except getopt.GetoptError as err:
        print("Error: %s\n" % err)

    aDict["ConfigFile"] = ConfigFile
    aDict["Mode"] = Mode 
    aDict["date_traitement"] = date_traitement 
    return aDict

# ---------------------------------------------------------------------------------
# [N°19] 
# 
# convert_seconds_to_hours_min
#
# 
# 
#         
# ---------------------------------------------------------------------------------
def convert_seconds_to_hours_min(seconds):
    minutes, sec = divmod(seconds, 60)
    hour, minutes = divmod(minutes, 60)
    return "%d:%02d:%02d" % (hour, minutes, sec)

# ---------------------------------------------------------------------------------
# [N°20] 
# 
# convert_seconds_to_hours_min
#
# 
# 
#         
# ---------------------------------------------------------------------------------
def ecriture_vue_temporarire_sur_disk(spakSession, df_vue_name, nom_database):
    err_log="Please, provide the name of database to use ecriture_vue_temporarire_sur_disk function\n"
    if nom_database:
        try:
            table = nom_database+"."+df_vue_name
            spakSession.sql( "DROP TABLE IF EXISTS "+table )
            spakSession.sql( "CREATE TABLE "+table )
            spakSession.catalog.uncacheTable(df_vue_name)
        except Exception as ex:
            print(str(ex))
            raise Exception( str(ex) )
    else:
        raise Exception(err_log)

# ---------------------------------------------------------------------------------
# [N°21] 
# 
# get_hue_table_schema()
#
# generate the schema of typ StructType 
# 
#         
# ---------------------------------------------------------------------------------
def get_hue_table_schema(
    spark:pyspark.sql.session.SparkSession=None,
    table_name:str=None,
    concerned_columns:[str, list,None] = None,
    show_create_table_file:str=None
) -> StructType :

    try:
        # ------------------------------------------------
        # if we know the source table name (from which we will build the schma)
        # and this table is accessible from our wrking env.
        # exemple your table is in hive(hue) and your are working on jupyter (or elsewhre)
        # -------------------------------------------------
        if show_create_table_file: #is given [this file can be in hql or csv format]
            nom_de_base_fichier_show_create, extension_fichier_show_create = os.path.splitext(show_create_table_file)
            print("\n\n#########################################################\n\
            # [Obtention du schema de la table {}: function get_hue_table_schema]\n\
            # A partir d'un {}\n\
            # fichier: {}\n\
            # #################################################################\n\n".format(table_name, extension_fichier_show_create, show_create_table_file))

        if (not show_create_table_file) and isinstance(spark,pyspark.sql.session.SparkSession ):
            req=''
            if concerned_columns is None or [] == concerned_columns or '' == concerned_columns:
                req = "SELCT * FROM "+table_name+" LIMIT 1"
            elif isinstance(concerned_columns, str) and "*" == concerned_columns :
                req = "SELCT * FROM "+table_name+" LIMIT 1"
            elif isinstance(concerned_columns, str):
                chosen_columns = concerned_columns 
                req = "SELCT "+chosen_columns+" FROM "+table_name+" LIMIT 1"
            elif isinstance(concerned_columns, list) or isinstance(concerned_columns, tuple):
                concerned_columns = list(set(concerned_columns))
                chosen_columns = ",".join(concerned_columns)
                req = "SELCT "+chosen_columns+" FROM "+table_name+" LIMIT 1"
            else:
                raise Exception("Unlnown concerned columns type in called function show_create_table_file argument")

            hdfs_schema = spark.sql( req ).schema
            return hdfs_schema
        elif isinstance(show_create_table_file, str):
            # ------------------------------------------------
            # if we only have the show create table fil and want to build the schema StructTyp
            # we procd like that
            # 
            # -------------------------------------------------
            cols_types = []
            with open(show_create_table_file, 'r') as fd:
                lines_tmp = fd.readlines()
                # sometime, if you copy show_create query result from hie
                # the number of lines appears, so we remove them before
                lines_tmp = [ re.sub(r'^\d+\s+', '', line.replace("'", "").replace('"', '')) for line in lines_tmp ]
                lines = []
                for line in lines_tmp:
                    # we don't need the line out of (or after); ROW FORMAT SERDE metadat
                    # so we drop all  the lines coming after it 
                    if line.strip().lower().startswith("row"):
                        break
                    elif line.startswith("-") or (not len(line.strip())): #ignore the commented line  or mpty line
                        pass 
                    elif 'CREATE' in line or "EXTERNAL" in line or "TABLE" in line or line.startswith(")"): #ignore these line
                        pass
                    else :
                        lines.append( line )
                for line in lines:
                    mytuple = tuple()
                    if 'varchar' in line  or 'decimal' in line or 'int' in line or 'date' in line:
                        splited = line.split()
                        if not len(splited):
                            raise Exception("[RROR] : while splitting col_nam/col_typ from called function get_hue_table_schema\n")
                        col_name = splited[0].strip().replace("`", "")

                        if 'date' in line:
                            mytuple = (col_name, 'date', 'unknown')
                        else:
                            raw_col_type = splited[1].strip().replace(",", "")
                            alpha_regex = "[A-Za-z]+"
                            num_regex = "[0-9]+"
                            col_type = re.findall(alpha_regex, raw_col_type)[0]
                            col_size = re.findall(num_regex, raw_col_type)[0]
                            mytuple = (col_name, col_type, col_size)

                        cols_types.append( mytuple )
                        #print(cols_types)
                        #Examples: cols_types:[('id', 'varchar', '5'), ('birth','date'), ...]
                    else:
                        raise Exception("Unknow data type from called function get_hue_table_schema\nUnable to parse {}".format(line))
            df_fields = []
            schema = ''

            if len(cols_types):
                for el in cols_types:
                    col_name, col_type, col_size =  el 
                    mystructField = ''
                    if col_type == 'date': 
                        mystructField = StructField("'"+col_name+"'", DateType(), True)
                    elif col_type == 'datetime' or col_type == 'timestamp' : 
                        mystructField = StructField("'"+col_name+"'", TimestampType(), True)
                    elif col_type == 'varchar' or col_type == 'text' : 
                        mystructField = StructField("'"+col_name+"'", StringType(), True)
                    elif col_type == 'decimal': 
                        mystructField = StructField("'"+col_name+"'", DecimalType(), True)
                    elif col_type == 'int':  
                        mystructField = StructField("'"+col_name+"'", IntegerType(), True)
                    elif col_type == 'float': 
                        mystructField = StructField("'"+col_name+"'", FloatType(), True)
                    elif col_type == 'binary' or col_type == 'blob' : 
                        mystructField = StructField("'"+col_name+"'", BinaryType(), True)
                    elif col_type == 'smallint' : 
                        mystructField = StructField("'"+col_name+"'", ShortType(), True)
                    elif col_type == 'bigint' : 
                        mystructField = StructField("'"+col_name+"'", LongType(), True)
                    else: 
                        raise Exception("[ERROR]:  unknown type from called function get_hue_table_schema ")
                    
                    df_fields.append( mystructField )
            
            if len(df_fields):
                return StructType( df_fields )

    except Exception as ex:
            print("[ERROR]: "+str(ex))
            return None 


# ---------------------------------------------------------------------------------
# [N°22] 
# 
# create_dataframe_base_on_schema_from_csv_file
#
# Provide csv data file and sho_create file and this script will build your dataframe 
# 
#         
# ---------------------------------------------------------------------------------
def create_dataframe_base_on_schema_from_csv_file(
    spark:pyspark.sql.session.SparkSession=None,
    csv_data_file:str=None,
    sep:str=",",
    hue_table_name:str=None,
    concerned_columns:[str, list,None] = None,
    show_create_table_file:str=None,
    is_local_csv_data_file:bool=True
) -> pyspark.sql.DataFrame:
    prepend = ''
    if is_local_csv_data_file:
        prepend = 'file://'
    
    df = None 
    if not hue_table_name and (not show_create_table_file):
        df = spark.read\
        .format("csv")\
        .option('header', 'true')\
        .option("quote", "")\
        .option("escape", "") \
        .option('delimier', sep)\
        .option('prefix', "false")\
        .load(prepend + csv_data_file)
    else:
        df_schema_to_apply = get_hue_table_schema(spark, hue_table_name, concerned_columns=concerned_columns,show_create_table_file=show_create_table_file)
        
        df = spark.read\
        .format("csv")\
        .option('header', 'true')\
        .option("quote", "") \
        .option("escape", "") \
        .schema(df_schema_to_apply) \
        .option('prefix', "false")\
        .load(prepend + csv_data_file)
    
    
    try: #sometime in csv (if it is for exemple a downloaded file), we columns name are prefixed by table_name , so let's remove it
        columns_names_without_table_name_at_the_begining = [column.split(".")[1] for column in df.columns]
        df = df.toDF(*columns_names_without_table_name_at_the_begining)
    except Exception as ex: 
        pass
    return df


# ---------------------------------------------------------------------------------
# [N°23] 
# 
# read_table_parquet_file_from_hdfs
#
# this script read parquet(s) file and convert it(them) to only one dataframes
# Juste give him the directory name and it will collect all parquets inside and read them
#         
# ---------------------------------------------------------------------------------

def read_table_parquet_file_from_hdfs(
    spark:pyspark.sql.session.SparkSession=None,
    parquet_directory_in_hdfs:str = None,
    lib=None,
    logger=None,
    show_result_dataframe_sample = False
) -> pyspark.sql.DataFrame:

    try:
        lib.write_data_info("\n\n##############################################################\n\
        # \n\
        # read_table_parquet_file_from_hdfs \n\
        # \n\
        # Check if this givn directory {} contains any parquet file(s)  \n\
        # \n\
        # hdfs dfs -ls -R "+parquet_directory_in_hdfs+" | grep '.parquet' | awk 'p r i n t   $ 8\' \n\
        # \n\
        # ########################################################################################\n".format(parquet_directory_in_hdfs), logger)
        
        #command = "hdfs dfs -ls -R "+parquet_directory_in_hdfs+" | grep '.parquet' | awk '{print $8}' "
        command ="hdfs dfs  -find "+ parquet_directory_in_hdfs + os.sep + " | grep par "
        execution = subprocess.Popen([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output,err = execution.communicate(b"find table parquet data")
        rc = execution.returncode
        lib.write_data_info( command, logger)
      
        if rc:
            lib.write_data_info(f"[ERROR]: \n\tNO PARQUET FILE FOUND IN: {parquet_directory_in_hdfs}", logger)
            lib.write_data_info(output.decode()+"\nsys.exit(1)", logger)
            sys.exit(1)
        else :
            lib.write_data_info("\nRESULT [OK found parquet(s) file(s)]\n"+output.decode(), logger)

        parquet_files_list = [el for el in output.decode().split('\n') if len(el) > 0]
        parDF1 = spark.read.parquet(*parquet_files_list)
        if show_result_dataframe_sample:
            lib.write_data_info(lib.getShowString(parDF1.limit(5)), logger)
        lib.write_data_info("\n\n##############################################################\n\
        # [->-> STEP END]\n\
        #                                   FIN DU SCRIPT read_table_parquet_file_from_hdfs \n\
        # \n\
        # \n\
        # ########################################################################################\n", logger)
        lib.write_data_info('\n\nFIN DU SCRIPT '+os.path.basename(__file__), logger)
        return parDF1
    except Exception as ex:
        print("[ERROR ]: Reading parquet from directory\n"+str(ex))
        return None
        


# ---------------------------------------------------------------------------------
# [N°24] 
# 
# read_csv_data_and_write_parquet_file_to_hdfs
#
# this script read a csv and tranform the read data to parquet and sd it to hdfs
# 
#         
# ---------------------------------------------------------------------------------
def read_csv_data_and_write_parquet_file_to_hdfs(
    spark:pyspark.sql.session.SparkSession=None,
    lib=None,
    logger:str=None,
    csv_data_file_abs_path:str=None,
    database_path_in_hdfs:str=None,
    table_name:str= None,
    is_local_running:str=None,
    remote_server:str='hdfs://localhost:9000'
) -> None:
    lib.write_data_info("\n\n##############################################################\n\
    # [->-> STEP 0]\n\
    # read_csv_data_and_write_parquet_file_to_hdfs \n\
    # \t\tthis script read a csv and tranform the read data to parquet and sd it to hdfs\n\
    # \n\
    # ########################################################################################\n", logger)
    time.sleep(1)

    lib.write_data_info("\n\n##############################################################\n\
    # [->-> STEP 1]\n\
    # Lecture du csv et extraction du dataframe  \n\
    # \tsauvegarde dans un repertoire temporarie {} \n\
    # \n\
    # ########################################################################################\n".format(csv_data_file_abs_path), logger)
    df_test = ''
    if is_local_running:
        df_test = spark.read.format("csv")\
        .option('header', 'true')\
        .option('prefix', "false")\
        .load('file://' + csv_data_file_abs_path)

        lib.write_data_info("Your CSV path is: \nfile://"+csv_data_file_abs_path, logger)
    else: 
        df_test = spark.read.format("csv")\
        .option('header', 'true')\
        .option('sep', ',') \
        .option('prefix', "false")\
        .load(csv_data_file_abs_path)

        lib.write_data_info("Your CSV path is: \n"+csv_data_file_abs_path, logger)
    
    try: #sometime in csv (if it is for exemple a downloaded file), we columns name are prefixed by table_name , so let's remove it
        columns_names_without_table_name_at_the_begining = [clumn.split(".")[1] for column in df_test.columns]
        df_test = df_test.toDF(*columns_names_without_table_name_at_the_begining)
    except Exception as ex: 
        pass
    lib.write_data_info("\n\nDebut Aperçu des 5 prmières lignes du CSV\n"+lib.getShowString(df_test.limit(5))+"Fin aperçu des 5 premières lignes du CSV\n", logger)
    csv_file_name = os.path.basename(csv_data_file_abs_path)
    lib.write_data_info("\n\n##############################################################\n\
    # [->-> STEP 2]\n\
    # Building hdfs parquet file path {} \n\
    # \n\
    # \n\
    # ########################################################################################\n".format( database_path_in_hdfs.rstrip('\/') + '/' + table_name + '/' + re.sub('csv', 'parquet', csv_file_name)  ), logger)
    hdfs_parquet_file = ''
    hdfs_table_path = database_path_in_hdfs.rstrip('\/') + '/' + table_name 
    if database_path_in_hdfs:
        hdfs_parquet_file = hdfs_table_path
    # --------------- remove old parque (if exists ) -------------
    command = "hdfs dfs -rm -r "+ hdfs_parquet_file+'  >> /dev/null' #overwrite is a copy of parquet which must be sent to hdfs 
    execution = subprocess.Popen([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output,err = execution.communicate(b"remove old parquet file ")
    rc = execution.returncode
    lib.write_data_info( command, logger)

    # build your daafram and save it to hdfs_parquet_file
    hdfs_alternative_parquet_path = '/data/tables/'+table_name 
    lib.write_data_info("\n\n##############################################################\n\
    # [->-> STEP 3]\n\
    # Saving parquet file into hdfs  \n\
    # try {} \n\
    # OR\n\
    # except {}\n\
    # ########################################################################################\n".format(hdfs_parquet_file, hdfs_alternative_parquet_path), logger)
    path_parquet = ''
    try:
        df_test.write.mode('overwrite').parquet(remote_server + hdfs_parquet_file)
        lib.write_data_info( "\n-->\ndf_test.write.mode('overwrite').parquet("+remote_server+hdfs_parquet_file+")" ,logger)
        # check if parquet are now in hdfs
        command2 = "hdfs dfs -ls "+ hdfs_parquet_file
        execution = subprocess.Popen([command2], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output,err = execution.communicate(b"detection automatique du user_id")
        rc = execution.returncode
        lib.write_data_info( "\n"+command2+"\n\n", logger)
        path_parquet = remote_server + hdfs_parquet_file
    except Exception as ex:
        lib.write_data_info("\n=========\n=====÷\n[ERROR]: \n", logger)
        lib.write_data_info("\n=========\n=====÷\n[ERROR]: \n", logger)
        lib.write_data_info(str(ex), logger)
        lib.write_data_info("\n=========\n=====\n[ERROR]: \n", logger)
        lib.write_data_info("\n=========\n=====\n[ERROR]: \n", logger)
        sys.exit(1)
    
    lib.write_data_info("\n\n##############################################################\n\
    # [->-> STEP 4]\n\
    # Here are your new parquets content \n\
    #  {} \n\
    # \n\
    # ########################################################################################\n".format(path_parquet), logger)
    df_from_parquet = lib.read_table_parquet_file_from_hdfs(spark,  path_parquet, lib, logger)
    lib.write_data_info("\ndf_from_parquet = lib.read_table_parquet_file_from_hdfs(spark, "+ path_parquet+", lib, logger)", logger)
    if not isinstance(df_from_parquet, pyspark.sql.DataFrame):
        lib.write_data_info("\n\n###################################################\n\
        # [ERROR STEP 4]\n\
        #  function read_table_parquet_file_from_hdfs \n\
        #  returns invalid dataframe instance  \n\
        # \n\
        # ########################################################################\nsys.exit(1)".format(path_parquet), logger)
        sys.exit(1)
    lib.write_data_info("\nStart Display PARQUET Content(sampl of 5 rows)\n"+lib.getShowString(df_from_parquet.limit(5))+"\nEnd Display PARQUET Content(sample of 5 rows)", logger  )

    lib.write_data_info("\n\n##############################################################\n\
    # [->-> STEP 5]\n\
    # Checkin if parquts are in hdfs path: {} \n\
    # \n\
    # \n\
    # ########################################################################################\n".format(path_parquet), logger)
    command_test = "hdfs dfs -ls "+ hdfs_alternative_parquet_path
    execution = subprocess.Popen([command_test], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output,err = execution.communicate(b"detection automatique du user_id")
    rc = execution.returncode
    lib.write_data_info( "\n"+command_test+"\n"+output.decode()+"\n", logger)

    lib.write_data_info("\n\n##############################################################\n\
    # [->-> STEP 6]\n\
    #                                   FIN DU SCRIPT read_csv_data_and_write_parquet_file_to_hdfs \n\
    # \n\
    # \n\
    # ########################################################################################\n", logger)
    lib.write_data_info('\n\nFIN DU SCRIPT '+os.path.basename(__file__), logger)
    lib.write_data_info("\n\nhive> LOAD DATA INPATH \"{}\" INTO TABLE {} ; \n\n".format(hdfs_parquet_file, table_name), logger)


# ---------------------------------------------------------------------------------
# [N°25] 
# concatenate_dataframes_as_text_file
# 
#
# 
# 
#         
# ---------------------------------------------------------------------------------
def concatenate_dataframes_as_text_file(
    spark, dataframes_list,
    text_file_abs_path,
    lib=None,
    logger=None,
    is_local_file=False, SEP=""):

    if (not isinstance(dataframes_list)) and (not isinstance(dataframes_list, tuple)):
        raise Exception("[ERROR]: Wrong parameters type - Pleas, set lis or tuple as arguments")
        exit(1)

    for df in dataframes_list:
        if False == isinstance(df, pyspark.sql.DataFrame):
            raise Exception("Wrong object type inside list - must be pyspark.sql.DataFrame\n")
            exit(1)
    # Now Create resililent distributed dataframe:
    if is_local_file:
        text_file_abs_path = "file://"+text_file_abs_path
    TOTAL_ROWS = 0
    TOTAL_COLUMNS = 2500
    TOTAL_ROWS_MULTIPLY_COLUMNS = 0
    concatenated_rdd = spark.sparkContext.emptyRDD()

    for idx, df in enumerate(dataframes_list):
        if not df.is_cached:
            df.cache()
        concatenated_rdd = concatenated_rdd.union(df.rdd)

        TOTAL_ROWS += df.count()
        TOTAL_ROWS_MULTIPLY_COLUMNS += (df.count() * len(df.columns) )
    # If there are many partitions, make sur to conver all of them to only one partition
    concatenated_rdd = concatenated_rdd.coalesce(1)
    # Now conver RDD of row (because rdd is store as row object)
    concatenated_rdd_as_text = concatenated_rdd.map(lambda row: SEP.join(map(str, row)))

    # before saving remove all files
    command_rm="hdfs dfs -rmr "+text_file_abs_path+"_tmp "+text_file_abs_path+" 2>/dev/null "
    lib.write_data_info("\n\n#########################################\n\
    # [2] Removing old file before saving new text file: \n"+command_rm+"\n\
    #\n\
    ##########################################", logger)
    execution = subprocess.Popen([command_rm], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, err = execution.communicate(b"remove file from hdfs if it exists ")
    rc = execution.returncode

    # Now save new file
    lib.write_data_info("\n\n#########################################\n\
    # [3] Start saving new text file: \n"+text_file_abs_path+"_tmp\n\
    #\n\
    ##########################################", logger)
    concatenated_rdd_as_text.saveAsTextFile( text_file_abs_path+"_tmp" )
    # Info the file will be saved into as directoy containing flag SUCCESS and par file
    # let's get the part file 
    command_part_file_name_cmd="hdfs dfs -ls "+text_file_abs_path+"_tmp | awk '{print $0}' | grep 'part'  "
    lib.write_data_info("\n\n#########################################\n\
    # [4] Tracking the name of part file: \n"+ command_part_file_name_cmd+"\n\
    #\n\
    ######################################", logger)
    execution = subprocess.Popen([command_part_file_name_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, err = execution.communicate(b"remove file from hdfs if it exists ")
    rc = execution.returncode

    part_file_name = ''
    part_file_basename = ''
    if not rc:
        part_file_basename = output.decode()
        part_file_name = re.sub("\n", "", part_file_name)
        part_file_basename = os.path.basename( part_file_name )

        lib.write_data_info("\n\n######################################\n\
         # [5] The part file path i get from previous command is : \n"+ part_file_name+"\n\
         #\n\
         # The part file basename i get from previous command is : \n"+part_file_basename+"\n\
         #\n\
         ########################################", logger)
    else:
        raise Excption("Unable to get part file from created directory "+text_file_abs_path+"_tmp")
        exit(1)
    command_mv = "hdfs dfs -mv "+text_file_abs_path+"_tmp/"+part_file_basename+" "+text_file_abs_path
    lib.write_data_info("\n\n#########################################\n\
    # [6] Now moving part file from "+ text_file_abs_path+"_tmp: \n"+command_mv+"\n\
    #\n\
    ######################################", logger)
    execution = subprocess.Popen([command_mv], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, err = execution.communicate(b"rmv file ")
    rc = execution.returncode
    if not rc:
        command_del = "hdfs dfs -rmr "+text_file_abs_path+"_tmp/"
        lib.write_data_info("\n\n##############################\n\
        # [7]Deleting "+ text_file_abs_path+"_tmp: \n"+command_del+"\n\
        #\n\
        ######################################", logger)
        execution = subprocess.Popen([command_del], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, err = execution.communicate(b"rmv file ")
        rc = execution.returncode
    else:
        raise Exception("Unable to mv part file from :"+text_file_abs_path+"_tmp")
        exit(1)

    # Checking long/shor line if exists: La VAR (mdr)
    command_check = "hdfs dfs -cat "+text_file_abs_path+" | awk 'BEGIN {max_length = 0; min_length = 99999999; line_count = 0} { len = length($0); if( len > max_length){max_length = len; max_line = $0; max_len = len} if(len < min_length){ min_len = len; mine_line = $0; min_len = len} line_count++ } END {print \"Le nombre de lignes est:\\n \", line_count; print \"\\nLigne la plus longue:\\n\", max_line, \"(\", max_len, \"characteres)\"; print \"\nLigne la plus courte:\\n\", min_line, \"(\", min_len, \"caracteres)\"}'  "
    lib.write_data_info("\n\n##############################\n\
    # [8] VERIFICATION: Recherche de la ligne la plus longue et la ligne la plus courte\n\
    #\n\
    # On doit avoir normalement 2500 pour les deux \n\
    #\n\
    # Command : \n" + command_check + "\n\
    #\n\
    ######################################", logger)
    execution = subprocess.Popen([command_check], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, err = execution.communicate(b"rcheck line number of cheracters ")
    rc = execution.returncode
    if not rc:
        lib.write_data_info("\n\n##############################\n\
        # [9] ====== RESULT CHECKING ====== \n"+output.decode()+"\n\
        #\n\
         ########################################", logger)


# ---------------------------------------------------------------------------------
# [N°26] 
# 
# 
#
# 
#         
# ---------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------
# [N°10] 
# 
# 
#
# 
# 
#         
# ---------------------------------------------------------------------------------