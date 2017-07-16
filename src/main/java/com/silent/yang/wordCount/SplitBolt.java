package com.silent.yang.wordCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by dell on 2017/7/15.
 */
public class SplitBolt implements IRichBolt {

    OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
       outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        for(String word : sentence.split("\\s+")) {
//            System.out.println(word);
            outputCollector.emit(new Values(word));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
