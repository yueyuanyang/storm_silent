package com.silent.yang.wordCount;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleHelpers;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dell on 2017/7/15.
 */
public class CountBolt implements IRichBolt {

    OutputCollector outputCollector;
    Map<String,Integer> counts = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
      this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        if(TupleHelpers.isTickTuple(input)) {
            /**
             * 定时发送下游
             */
            outputCollector.emit(new Values(counts));
            /**
             * 这个地方，不能执行clear方法，可以再new一个对象，
             * 否则下游接受的数据，有可能为空 或者深度copy也行，推荐new
             */
            counts=new HashMap<>();
            return;
        }
        int count;
        String word = input.getString(0);
        if(counts.containsKey(word)) {
            count = counts.get(word);
        }else {
            count = 0;
        }
        count=count+1;
        counts.put(word,count);
//        System.out.println("---------------");
//        System.out.println(word + "  " + count);
//        outputCollector.emit(new Values(word));

//        BufferedWriter out = null;
//        try {
//            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("E:/storm.txt" , true)));
//            out.write(input.getSourceComponent()+"-->"+word+"\r\n");
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                if (null != out) {
//                    out.close();
//                }
//            } catch (IOException io) {
//                io.printStackTrace();
//            }
//        }

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
        /**
         * 加入Tick时间窗口，进行统计
         */
        Map<String,Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,3);
        return conf;
    }
}
