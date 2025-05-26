#添加

mvn install -Dmaven.test.skip=true -Dfast -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-kingbase-cdc\
,flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-paimon
