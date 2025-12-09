#添加

mvn clean deploy  -Dspotless.check.skip=true -Dmaven.test.skip=true -Ptis,docs-and-source -Dfast \
-pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-kingbase-cdc\
,flink-cdc-connect/flink-cdc-source-connectors/flink-connector-oracle-cdc\
,flink-cdc-connect/flink-cdc-source-connectors/flink-connector-dameng-cdc\
,flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-paimon\
,flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc\
,flink-cdc-composer
