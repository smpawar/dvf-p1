#!/bin/bash

echo "running dvt in shell"

set -e

if [ $2 = "bigquery" ]; then
    source_conn="bq_conn"
elif [ $2 = "teradata" ]; then
    source_conn="td_conn"
fi

if [ $3 = "bigquery" ]; then
    target_conn="bq_conn"
elif [ $3 = "teradata" ]; then
    target_conn="td_conn"
fi

	
if [ $1  = "count" ]; then
    echo "executing column validation"
    command="data-validation validate column --source-conn $source_conn --target-conn $target_conn --tables-list $4=$5 --bq-result-handler $6"
    echo $command
	eval $command

elif [ $1 = "row" ]; then
    echo "executing row validation"
    if [ $7 = "Y" ]; then
        command="data-validation validate row --source-conn $source_conn --target-conn $target_conn --tables-list $4=$5 --primary-keys $6 --hash $8 --exclude-columns --bq-result-handler $9 --trim-string-pks"
    else 
        command="data-validation validate row --source-conn $source_conn --target-conn $target_conn --tables-list $4=$5 --primary-keys $6 --hash '*' --bq-result-handler $8 --trim-string-pks"
    fi
    echo $command
    eval $command

elif [ $1 = "partition" ]; then
    echo "generating partitioned yamls"
    if [ $7 = "Y" ]; then
        command="data-validation generate-table-partitions --source-conn $source_conn --target-conn $target_conn --tables-list $4=$5 --primary-keys $6 --hash $8 --exclude-columns --bq-result-handler $9 --trim-string-pks --partition-num ${10} --parts-per-file ${11} --config-dir ${12}"
    else
        command="data-validation generate-table-partitions --source-conn $source_conn --target-conn $target_conn --tables-list $4=$5 --primary-keys $6 --hash '*' --bq-result-handler $8 --trim-string-pks --partition-num $9 --parts-per-file ${10} --config-dir ${11}"
    fi
    echo $command
    eval $command

elif [ $1 = "custom_no_partition" ]; then
    echo "executing custom query validation"
    if [ $5 = "Y" ]; then
        command="data-validation validate custom-query row --source-conn $source_conn --target-conn $target_conn --primary-key $4 --hash $6 --exclude-columns --source-query-file $7 --target-query-file $8 --bq-result-handler $9 --trim-string-pks"
    else 
        command="data-validation validate custom-query row --source-conn $source_conn --target-conn $target_conn --primary-key $4 --hash '*' --source-query-file $6 --target-query-file $7 --bq-result-handler $8 --trim-string-pks"
    fi
    echo $command
    eval $command

elif [ $1 = "custom_partition" ]; then
    echo "generating partition yamls for custom query validation"
    if [ $5 = "Y" ]; then
        command="data-validation generate-table-partitions --source-conn $source_conn --target-conn $target_conn --primary-key $4 --source-query-file $6 --target-query-file $7 --hash $8 --exclude-columns --bq-result-handler $9 --trim-string-pks --partition-num ${10} --parts-per-file ${11} --config-dir ${12}"
    else
        command="data-validation generate-table-partitions --source-conn $source_conn --target-conn $target_conn --primary-key $4 --hash '*' --source-query-file $6 --target-query-file $7 --bq-result-handler $8 --trim-string-pks --partition-num $9 --parts-per-file ${10} --config-dir ${11}"
    fi
    echo $command
    eval $command    

fi

exit 0
