# setup

docker compose up 

# 验证 topic 是否创建成功 

docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# setup flink

mvn archetype:generate -DgroupId=com.example -DartifactId=flink-kafka-integration -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
cd flink-kafka-integration

# build flink 

mvn clean package

# flink run job

docker exec -it flink-jobmanager /bin/bash
./bin/flink run ./jars/flink-kafka-integration-1.0.jar 
