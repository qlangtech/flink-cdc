#添加

mvn clean deploy   -Dmaven.test.skip=true -Ptis -Dfast \
-pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-kingbase-cdc\
,flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-paimon\
,flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc\
,flink-cdc-composer
