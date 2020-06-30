#!/bin/bash
function remove_dirs
{
        USAGE="Usage: ./list-old-hdfs-files.sh [path] [days]"

        #if number of arguments is less than 2 USAGE description of this progam is redirected to the screen
        if [ $# -ne 2 ];
        then
          echo "$USAGE"
          exit 0
        else

        #NOW refers to the current_time in epoch seconds
        #RETENTION_DAYS referes to the prior to how many days old data needs to be purged

        NOW=$(date +%s);
        RETENTION_DAYS=$2
        TOTAL_RETENTION_SECONDS=$(( 24*60*60*$RETENTION_DAYS ))

        # Loop recursively through the files or directories listed in the operation hdfs dfs -ls -R $1
        # and perform the operations specifed on each line of the output

        hdfs dfs -ls -R $1 | while read f; do

        # Extract FILE_DATE and FILE_NAME

        FILE_DATE=`echo $f | awk '{print $6,$7}'`;
        FILE_NAME=`echo $f | awk '{print $8}'`;

        #Following calculation provides the difference between
        #current_time and file_time in epoch seconds

        DIFFERENCE_IN_TIME=$((( $NOW - $(date -d "$FILE_DATE" +%s ))));

        # Delete the file if the file date is greater than the retention period

        if [[ $DIFFERENCE_IN_TIME -gt $TOTAL_RETENTION_SECONDS ]]; then
                echo -e "\nStarted Deleting the file $FILE_NAME is dated $FILE_DATE on `date +%FT%T%Z`\n"
                hadoop fs -rm -r -skipTrash $FILE_NAME

        fi

        done >> $log_path/$log_file 2>&1

        # $? expands to the exit status of the previously executed command
        if [ $? -ne 0 ];
        then
           send_email_on_failure
        else
           send_email_on_success
        fi
   fi
}
function write_to_log
{
        echo $@: "`date +%FT%T%Z`" >> $log_path/$log_file  2>&1

}
function send_email_on_success
{
    write_to_log "purge process succeeded"
    mail -s "$email_sub" "$email_id" < $log_path/$log_file

}
function send_email_on_finish
{

   mail -s "$email_sub" "$email_id" < $log_path/$log_file

}
function send_email_on_failure
{
    write_to_log "purge process failed"
    mail -s "$email_sub" "$email_id" < $log_path/$log_file

}
function initialize
{

    write_to_log "Initialization process started"
    #Create log directory if not exists and if it exists then delete log files which are older than 7 days
    if [[ ! -d $log_path ]]; then
        mkdir -p $log_path
            if [ $? -ne 0 ]; then
                echo "Unable to create log directory path ${log_path}"
                exit 10
            fi
            write_to_log "Log directory ${log_path} created"
        else
            find $log_path -type f -mtime +$log_retention_days -exec rm -f {} \;
        fi
}
config_file=$1
source ${config_file}
initialize "Purge Process inititated"
remove_dirs ${hdfs_dir} ${retention_days}