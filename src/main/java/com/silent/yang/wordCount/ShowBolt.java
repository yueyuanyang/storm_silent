package com.silent.yang.wordCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dell on 2017/7/16.
 */
public class ShowBolt implements IRichBolt {

    OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        /**
         *  实际应用中，最后一个阶段，大部分应该是持久化到
         *  mysql，redis，es，solr或mongodb中
         */
        Map<String,Integer> counts = (Map<String, Integer>) input.getValue(0);
        for(Map.Entry<String,Integer> kv:counts.entrySet()){
            System.out.println(kv.getKey()+"  "+kv.getValue());
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
