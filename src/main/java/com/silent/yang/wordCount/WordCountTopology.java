package com.silent.yang.wordCount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by dell on 2017/7/15.
 */
public class WordCountTopology {

    public static final Logger logger = LoggerFactory.getLogger(WordCountTopology.class);

    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new Spout(),1);
        /***
         * shuffleGrouping 它只有一个参数（数据源组件），并且数据源会向随机选择的bolt发送元组，
         * 保证每个消费者收到近似数量的元组。
         */
        builder.setBolt("split",new SplitBolt(),1).shuffleGrouping("spout");
        /**
         * fieldsGrouping 域数据流组允许你基于元组的一个或多个域控制如何把元组发送给bolts。
         * 它保证拥有相同域组合的值集发送给同一个bolt。
         * 注: 在域数据流组中的所有域集合必须存在于数据源的域声明中
         */
        builder.setBolt("count",new CountBolt()).fieldsGrouping("split",new Fields("word"));
        builder.setBolt("show",new ShowBolt()).allGrouping("count");
        Config config = new Config();
        config.setDebug(false);

        if (args != null && args.length > 0) {
            config.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
        else {
            /*
             * run in local cluster, for test in eclipse.
             */
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", config, builder.createTopology());
            Thread.sleep(Integer.MAX_VALUE);
            cluster.shutdown();
        }
    }
}
