#!/bin/bash 
#---------------------------------------------------------------------------------------------#
#  oozie_copy_scripts_wrks_launchers_to_hdfs.sh
#
#     [This script]: will copy to hdfs:
#                     - python pyspark script
#                     - shell scripts
#                     -------------------------
#                     - alse copy the workflows
#                     - the launchers
#                     -------------------------
#                     - The dependencies (utilities and json files needed)
#
#
#           [  Before running this script  ]:
#                1. start hadoop:  [alias]: sh inside zprofile
#                
#          [Command]: ./oozie_copy_scripts_wrks_launchers_to_hdfs.sh
#
#
#  By Ibrahima  TRAORE
#--------------------------------------------------------------------------------------------


CMD="rm -rf /opt/BIG_DATA_LIBS/oozie-4.3.0/logs/*"
echo -e "$CYAN [ 1 ]: $LGRAY $CMD $RESET \n"
eval "$CMD"

# delete old oozie logs
CMD="find  /opt/BIG_DATA_LIBS/oozie-4.3.0/logs -name "oozie.log-*" -exec readlink -f {} \;"
echo -e "$CYAN [ 2 ]: $LGRAY $CMD $RESET \n"
eval "$CMD"

CMD="hdfs dfs -chmod -R 755 /user/oozie/share/lib"
echo -e "$CYAN [ 3 ]: $LGRAY $CMD $RESET \n"
eval "$CMD"


CMD="echo \"Start creating directories and copying scripts and workflows\""
echo -e "$CYAN [ 4 ]: $LGRAY $CMD $RESET \n"
eval "$CMD"

hdfs dfs -mkdir -p /user/hadoop/oozie-ibra
hdfs dfs -chmod -R 775 /user/hadoop/oozie-ibra
hdfs dfs -chown -R hadoop:supergroup /user/hadoop/oozie-ibra

hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/hadoop/.staging
hdfs dfs -chmod -R 775 /tmp/hadoop-yarn/staging/hadoop/.staging
hdfs dfs -chown -R hadoop:supergroup /tmp/hadoop-yarn/staging/hadoop/.staging

# in local
mkdir -p /tmp/hadoop/logs/nodemanager
chmod -R 755 /tmp/hadoop/logs/nodemanager

# Your taf
hdfs dfs -rm -r  /user/RUNS_OOZIE/workflows/  2>/dev/null
hdfs dfs -mkdir -p /user/RUNS_OOZIE/workflows  

hdfs dfs -rm -r /user/RUNS_OOZIE/workflows/workflow_shell_single_action.xml  2>/dev/null
hdfs dfs -put workflow_shell_single_action.xml /user/RUNS_OOZIE/workflows
hdfs dfs -rm -r /user/RUNS_OOZIE/workflows/workflow_shell_multi_actions.xml  2>/dev/null
hdfs dfs -put workflow_shell_multi_actions.xml /user/RUNS_OOZIE/workflows

hdfs dfs -rm -r /user/RUNS_OOZIE/workflows/workflow_python_pyspark_single_action.xml  2>/dev/null
hdfs dfs -put workflow_python_pyspark_single_action.xml /user/RUNS_OOZIE/workflows
hdfs dfs -rm -r /user/RUNS_OOZIE/workflows/workflow_python_pyspark_multi_actions.xml  2>/dev/null
hdfs dfs -put workflow_python_pyspark_multi_actions.xml /user/RUNS_OOZIE/workflows


hdfs dfs -rm -r /user/RUNS_OOZIE/launchers_pyspark/launch_only_python_pyspark_wrk.sh 2>/dev/null
hdfs dfs -mkdir -p /user/RUNS_OOZIE/launchers_pyspark 2>/dev/null
hdfs dfs -put launch_only_python_pyspark_wrk.sh /user/RUNS_OOZIE/launchers_pyspark

hdfs dfs -rm -r /user/RUNS_OOZIE/launchers_pyspark/launch_python_or_shell_wrks.sh 2>/dev/null
hdfs dfs -mkdir -p /user/RUNS_OOZIE/launchers_pyspark 2>/dev/null
hdfs dfs -put launch_python_or_shell_wrks.sh /user/RUNS_OOZIE/launchers_pyspark

hdfs dfs -rm -r /user/RUNS_OOZIE/json/file.json 2>/dev/null
hdfs dfs -mkdir -p /user/RUNS_OOZIE/json/ 2>/dev/null
hdfs dfs -put file.json /user/RUNS_OOZIE/json

hdfs dfs -rm -r /user/RUNS_OOZIE/scripts/shell/script.sh 2>/dev/null
hdfs dfs -mkdir -p /user/RUNS_OOZIE/scripts/shell 2>/dev/null
hdfs dfs -put shell-scripts/script.sh /user/RUNS_OOZIE/scripts/shell   

hdfs dfs -rm -r /user/RUNS_OOZIE/scripts/shell/date.sh 2>/dev/null
hdfs dfs -mkdir -p /user/RUNS_OOZIE/scripts/shell 2>/dev/null
hdfs dfs -put shell-scripts/date.sh /user/RUNS_OOZIE/scripts/shell  

hdfs dfs -rm -r /user/RUNS_OOZIE/scripts/python/create_dataframe.py  2>/dev/null
hdfs dfs -mkdir -p /user/RUNS_OOZIE/scripts/python 2>/dev/null
hdfs dfs -put python-scripts/create_dataframe.py  /user/RUNS_OOZIE/scripts/python  

hdfs dfs -rm -r /user/RUNS_OOZIE/scripts/python/read_dataframe.py  2>/dev/null
hdfs dfs -mkdir -p /user/RUNS_OOZIE/scripts/python 2>/dev/null
hdfs dfs -put python-scripts/read_dataframe.py  /user/RUNS_OOZIE/scripts/python  

hdfs dfs -rm -r /user/RUNS_OOZIE/scripts/python/dependencies  2>/dev/null
hdfs dfs -mkdir -p /user/RUNS_OOZIE/scripts/python/dependencies 2>/dev/null
hdfs dfs -put dependencies  /user/RUNS_OOZIE/scripts/python

CMD="echo \"+> +> echo Finish all tasks\""
echo -e "$CYAN [ 5 ]: $LGRAY $CMD $RESET \n"
eval "$CMD"

CMD="hdfs dfs -ls -R  /user/RUNS_OOZIE "
echo -e "$CYAN [ § ]: $LGRAY $CMD $RESET \n"
eval "$CMD"
