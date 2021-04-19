package fr.quentinpigne;

import fr.quentinpigne.bolts.PrinterBolt;
import fr.quentinpigne.spouts.CounterSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class CounterTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("counter-spout", new CounterSpout());
        topologyBuilder.setBolt("printer-bolt", new PrinterBolt());

        StormTopology topology = topologyBuilder.createTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("counter-topology", new Config(), topology);
    }
}
