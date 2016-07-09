package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/*
 * 1.spout对象必须是继承Serializable， 因此要求spout内所有数据结构必须是可序列化的
 * 2.spout可以有构造函数，但构造函数只执行一次，是在提交任务时，创建spout对象,
 * 因此在task分配到具体worker之前的初始化工作可以在此处完成，
 * 一旦完成，初始化的内容将携带到每一个task内
 * （因为提交任务时将spout序列化到文件中去，在worker起来时再将spout从文件中反序列化出来）
 */
public class RaceSentenceSpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(RaceSentenceSpout.class);
    SpoutOutputCollector _collector;
    Random _rand;
    long sendingCount;
    long startTime;
    boolean isStatEnable;
    int sendNumPerNexttuple;

    private static final String[] CHOICES = {"marry had a little lamb whos fleese was white as snow",
            "and every where that marry went the lamb was sure to go",
            "one two three four five six seven eight nine ten",
            "this is a test of the emergency broadcast system this is only a test",
            "peter piper picked a peck of pickeled peppers"};

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        sendingCount = 0;
        startTime = System.currentTimeMillis();
        sendNumPerNexttuple = JStormUtils.parseInt(conf.get("send.num.each.time"), 1);
        isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);
    }

    @Override
    public void nextTuple() {
        int n = sendNumPerNexttuple;
        while (--n >= 0) {
            Utils.sleep(10);
            String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
            _collector.emit(new Values(sentence));
        }
        updateSendTps();
    }

    @Override
    public void ack(Object id) {
        // Ignored
    }

    @Override
    public void fail(Object id) {
        _collector.emit(new Values(id), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    private void updateSendTps() {
        if (!isStatEnable)
            return;

        sendingCount++;
        long now = System.currentTimeMillis();
        long interval = now - startTime;
        if (interval > 60 * 1000) {
        	//模拟每秒钟发送了多少数据，sendingCount为本程序中发送的句子数，sendNumPerNexttuple为模拟的每次调nextTuple时的数据
            LOG.info("Sending tps of last one minute is " + (sendingCount * sendNumPerNexttuple * 1000) / interval);
            startTime = now;
            sendingCount = 0;
        }
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}