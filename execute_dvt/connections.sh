#!/bin/bash

echo "creating DVT connections"

set -e

# orahost=ora19c3
# oraport=1521
# orauser=hr
# orapw=hr
# oradb=

# pghost=${echo $PG_HOST}
# pgport=${echo $PG_PORT}
# pguser=${echo $PG_USER}
# pgpw=${echo $PG_PASSWORD}

# orahost=${echo $ORA_HOST}
# oraport=${echo $ORA_PORT}
# orauser=${echo $ORA_USER}
# orapw=${echo $ORA_PASSWORD}

# pghost=${echo $PG_HOST}
# pgport=${echo $PG_PORT}
# pguser=${echo $PG_USER}
# pgpw=${echo $PG_PASSWORD}

# terahost=${echo $TERA_HOST}
# teraport=${echo $TERA_PORT}
# terauser=${echo $TERA_USER}
# terapw=${echo $TERRA_PASSWORD}

echo "Create Oracle and PG connections"
# bqcommand="data-validation connections add --connection-name bq_conn BigQuery --project-id $1"

oracommand="data-validation connections add --connection-name ORA19C3 Oracle  --user hr --password hr --host 10.128.15.239 --port 1521 --database PROD1"
echo $oracommand
eval $oracommand

pgcommand="data-validation connections add --connection-name hr Postgres --host 146.148.94.56 --port 5432 --user postgres --password Oracle12! --database hr"
echo $pgcommand
eval $pgcommand

# echo "Teradata connection"
# tdcommand="data-validation connections add --connection-name td_conn Teradata --host $terahost --port $teraport --user-name $terauser --password $terapw"
# eval $tdcommand

# echo "Testing FileSystem src connection"
# fscommand="data-validation connections add --connection-name FILE_conn1 FileSystem --table-name file1_table --file-path  gs://dvt_filesystem_conn_test1/test_src.csv --file-type csv"
# echo $fscommand
# eval $fscommand

# echo "Testing FileSystem tgt connection"
# fscommand="data-validation connections add --connection-name FILE_conn2 FileSystem --table-name file2_table --file-path  gs://dvt_filesystem_conn_test1/test_tgt.csv --file-type csv"
# echo $fscommand
# eval $fscommand

# exit 0
