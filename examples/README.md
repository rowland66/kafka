# Kafka client examples

This module contains some Kafka client examples.

1. Start a Kafka 2.5+ local cluster with a plain listener configured on port 9092.
2. Run `examples/bin/java-producer-consumer-demo.sh 10000` to asynchronously send 10k records to topic1 and consume them.
3. Run `examples/bin/java-producer-consumer-demo.sh 10000 sync` to synchronous send 10k records to topic1 and consume them.
4. Run `examples/bin/exactly-once-demo.sh 6 3 10000` to create input-topic and output-topic with 6 partitions each,
   start 3 transactional application instances and process 10k records.

   KAFKA_CLUSTER_ID="$(./bin/kafka-storage.sh random-uuid)"
   ./bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
   ./bin/kafka-server-start.sh config/kraft/server.properties
