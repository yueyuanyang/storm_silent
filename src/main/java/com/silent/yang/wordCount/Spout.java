package com.silent.yang.wordCount;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.Map;
import java.util.Random;

/**
 * Created by dell on 2017/7/15.
 */
public class Spout implements IRichSpout{

    SpoutOutputCollector  spoutOutputCollector;
    Random random;
    long sendingCount;
    long startTime;
    boolean isStatEnable;
    int sendNumPerMexttuple;
    int arrayPosition;
    int arrayLength;

    private static final String[] CHOICES ={"a b","b a","a b"};


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        spoutOutputCollector = collector;
        random = new Random();
        sendingCount = 0;
        startTime = System.currentTimeMillis();
        sendNumPerMexttuple = JStormUtils.parseInt(conf.get("send.num.each.time"), 1);
        isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);
        arrayPosition = JStormUtils.parseInt(conf.get("array.position"),0);
        arrayLength = CHOICES.length;
    }

    @Override
    public void nextTuple() {

        if(arrayPosition <= arrayLength-1) {
            String sentence = CHOICES[arrayPosition];
            spoutOutputCollector.emit(new Values(sentence));
            arrayPosition++;
        }

        Utils.sleep(1000);
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }
}
