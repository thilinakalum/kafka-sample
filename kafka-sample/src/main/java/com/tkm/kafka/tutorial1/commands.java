//-- start zookeeper
// bin/zookeeper-server-start.sh config/zookeeper.properties

//-- start kafka server
// bin/kafka-server-start.sh config/server.properties

//-- create topic
// bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic menu_created --create --partitions 3 --replication-factor 1

//-- open console producer
// bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic menu_created

//-- open console consumer
// bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic menu_created

//-- open console consumer from the beginning
// bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic menu_created --from-beginning