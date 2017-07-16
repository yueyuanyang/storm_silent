package com.silent.yang.Jstrom;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by dell on 2017/7/15.
 */

public class TopologyTest {

    private final static Logger logg = LoggerFactory.getLogger(TopologyTest.class);

    private static TopologyBuilder builder = new TopologyBuilder();

    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {

        Config config = new Config();

        builder.setSpout("RandomSentence", new RandomSentenceSpout(), 2);
        builder.setBolt("WordNormalizer", new PrintBolt(), 2).shuffleGrouping("RandomSentence");


        config.setDebug(false);

        if(args != null && args.length > 0) {
            config.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }else {
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount", config, builder.createTopology());
            Thread.sleep(Integer.MAX_VALUE);
            //关闭本地集群模式
            cluster.shutdown();
        }
    }
}
