package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/*
 * 1.bolt对象必须是继承Serializable， 因此要求spout内所有数据结构必须是可序列化的
 * 2.bolt可以有构造函数，但构造函数只执行一次，是在提交任务时，创建bolt对象，
 * 因此在task分配到具体worker之前的初始化工作可以在此处完成，一旦完成，
 * 初始化的内容将携带到每一个task内
 * （因为提交任务时将bolt序列化到文件中去，在worker起来时再将bolt从文件中反序列化出来）。
 */
public class SplitSentence implements IRichBolt {
    OutputCollector collector;

  //execute是bolt实现核心， 完成自己的逻辑，即接受每一次取消息后，处理完，有可能用collector 将产生的新消息emit出去。
    //** 在executor中，当程序处理一条消息时，需要执行collector.ack
    //** 在executor中，当程序无法处理一条消息时或出错时，需要执行collector.fail 
    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        for (String word : sentence.split("\\s+")) {
            collector.emit(new Values(word));
        }
        collector.ack(tuple);
    }

  //declareOutputFields， 定义bolt发送数据，每个字段的含义
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

  //prepare是当task起来后执行的初始化动作
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

  //cleanup是当task被shutdown后执行的动作
    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
