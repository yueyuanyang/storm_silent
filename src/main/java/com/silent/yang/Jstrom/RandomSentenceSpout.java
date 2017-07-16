package com.silent.yang.Jstrom;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by dell on 2017/7/15.
 */
// 随机发送一条内置消息，该spout继承BaseRichSpout/IRichSpout类
@SuppressWarnings("serial")
public class RandomSentenceSpout extends BaseRichSpout {

    SpoutOutputCollector spoutOutputCollector;
    Random random;

    // 进行spout的一些初始化工作，包括参数传递
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        spoutOutputCollector = collector;
        random = new Random();
    }

    // 进行Tuple处理主要方法
    public void nextTuple() {
        Utils.sleep(200);
        String[] sentences = new String[]{
                "jikexueyuan is a good school",
                "And if the golden sun",
                "four score and seven years ago",
                "storm hadoop spark hbase",
                "blogchong is a good man",
                "Would make my whole world bright",
                "blogchong is a good website",
                "storm would have to be with you",
                "Pipe to subprocess seems to be broken No output read",
                " You make me feel so happy",
                "For the moon never beams without bringing me dreams Of the beautiful Annalbel Lee",
                "Who love jikexueyuan and blogchong",
                "blogchong.com is Magic sites",
                "Ko blogchong swayed my leaves and flowers in the sun",
                "You love blogchong.com", "Now I may wither into the truth",
                "That the wind came out of the cloud",
                "at backtype storm utils ShellProcess",
                "Of those who were older than we"};
        // 从sentences数组中，随机获取一条语句，作为这次spout发送的消息
        String sentence = sentences[random.nextInt(sentences.length)];
//        System.out.print("选择的数据源是: " + sentence);

        //使用emit 方法进行Tuple发布参数用Values申明
        spoutOutputCollector.emit(new Values(sentence.trim().toLowerCase()));

    }
    // 消息保证机制中的ack确认方法
    public void ack(Object id) {
    }

    // 消息保证机制中的fail确认方法
    public void fail(Object id) {
    }
    // 声明字段
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
