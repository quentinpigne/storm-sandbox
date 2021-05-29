package fr.quentinpigne;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Properties;

public class KafkaTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // KafkaSpout config
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder("kafka01:9091", "spout-topic");
        spoutConfigBuilder.setProp("group.id", "test-group");
        KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();

        topologyBuilder.setSpout("kafka-spout", new KafkaSpout<>(spoutConfig));

        // KafkaBolt config
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka01:9091");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>().withProducerProperties(props).withTopicSelector("bolt-topic")
            .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
        topologyBuilder.setBolt("kafka-bolt", kafkaBolt).shuffleGrouping("kafka-spout");

        String topologyName = "kafka-topology";
        Config config = new Config();
        StormTopology topology = topologyBuilder.createTopology();
        if (args.length > 0 && args[0].equals("local")) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, topology);
        } else {
            StormSubmitter.submitTopology(topologyName, config, topology);
        }
    }
}
