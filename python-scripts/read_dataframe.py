# ------------------------------------------------------------------------------
#  read_dataframe.py
#
#
#
#      spark-submit --queue default --master local --num-executors 10 --executor-memory 12g --executor-cores 4 --driver-memory 12g read_dataframe.py
#
# ####################################################################################
import sys
import os 
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
COLOR_FAILED = False 
try:
    from coloroma import Fore, Back, Style
except ModuleNotFoundError:
    COLOR_FAILED = True

encoding_id = "ISO-8859-1"

# Call you lib
sys.path.append( os.path.join( os.getcwd() + "/../dependencies" ))
import utilities as lib

# Create Spark Session

sparkSession = lib.init_spark( app_name = "read_dataframe.py" )
logger = lib.init_logger('info')

#sparkSession.sparkContext.setLogLevel("INFO")

globalStartTime = time.time()

if os.path.isfile('logs_'+os.path.basename(__file__)+'.txt'):
    open('logs_'+os.path.basename(__file__)+'.txt', 'w').close()


lib.write_data_info("###############################################\n\
# [ STEP 1]\n\
#  DEBUT DU SCRIPT "+os.path.basename(__file__) +"\n\
#\n\
#####################################################################", logger)

def main():
    (
        hdfs_logs_file,
        hdfs_today_folder,
        logs_file,
        user_id
    ) = lib.initialize_paramas()

    lib.write_data_info("###############################################\n\
    # [ STEP 1]\n\
    # READ DATAFRAME\n\
    #\n\
    #####################################################################", logger)

    try:
       
        parquet_path = "hdfs://localhost:9000/user/data/p_df1"
        df = sparkSession.read.parquet(parquet_path)\
        .select(
            "name", "gender", "weeding"
        )\
        .filter(
            "gender = 'F'"
        )

        # Show DataFrame
        lib.write_data_info(lib.getShowString(df), logger)


    except Exception as ex:
        print(ex)
        lib.error_exit(logger, logs_file, hdfs_logs_file, ex, additive_infos="Unexpected error")
        exit()

    ############################################################################
    #     FIN DU SCRIPT 
    ###########################################################################
    lib.write_data_info("###############################################\n\
    # [ STEP 1]\n\
    #  FIN  DU SCRIPT "+os.path.basename(__file__) +"\n\
    #\n\
    #####################################################################", logger)
    duree_en_seconds = time.time() - globalStartTime
    duree_hh_mm_ss = lib.convert_seconds_to_hours_min( duree_en_seconds )
    print('---------------------------------------------------------\n')
    if not COLOR_FAILED:
        print( Back.GREEN + Fore.WRITE + os.path.basename(__file__) + ": Succeed "+ Style.RESET_ALL)
        print("\n"+ Back.BLUE + Fore.WHITE + "duree_hh_mm_ss: "+duree_hh_mm_ss+Style.RESET_ALL+"\n")
    else:
        print( os.path.basename(__file__) + ": Succeed ")
        print("\nduree_hh_mm_ss: "+duree_hh_mm_ss+"\n")

    sparkSession.stop()
    lib.write_data_info("###############################################\n\
    # [ STEP 1]\n\
    #  SENDING LOGS FILE "+logs_file+" TO HDFS  "+hdfs_logs_file+ "\n\
    #\n\
    #####################################################################", logger)
    lib.copy_file_to_hdfs(logs_file, hdfs_logs_file)


if __name__ == "__main__":
    main()
