package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);


    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        //并发数
        int consumerDemoSpout_Parallelism_hint = 2;
        int ratioCalculate_Parallelism_hint = 1;
        int messageTagsDeal_Parallelism_hint = 6;
        int messageCount_Parallelism_hint = 6;

        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("ConsumerDemoSpout", new ConsumerDemoSpout(), consumerDemoSpout_Parallelism_hint);//(id,spout实例，并发设置)
        builder.setBolt("RatioCalculate", new RatioCalculate(), ratioCalculate_Parallelism_hint).shuffleGrouping("ConsumerDemoSpout", RaceConfig.ratioBoltStreamId);
        builder.setBolt("MessageTagsDeal", new MessageTagsDeal(), messageTagsDeal_Parallelism_hint).fieldsGrouping("ConsumerDemoSpout", RaceConfig.messageBoltStreamId, new Fields("orderId"));
        //builder.setBolt("MessageTagsDeal", new MessageTagsDeal(), messageTagsDeal_Parallelism_hint).fieldsGrouping("ConsumerDemoSpout", new Fields("orderId"));
        //表示接收SequenceTopologyDef.SEQUENCE_SPOUT_NAME的数据，并且以shuffle方式,即每个spout随机轮询发送tuple到下一级bolt中
        builder.setBolt("MessageCount", new MessageCount(), messageCount_Parallelism_hint).fieldsGrouping("MessageTagsDeal", new Fields("timestamp"));
        String topologyName = RaceConfig.JstormTopologyName;
   
        //表示整个topology将使用几个worker
        conf.setNumWorkers(4);
        conf.setNumAckers(2);
        //设置spout中存在最大的tuple数量
        //conf.setMaxSpoutPending(15000);
        //conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        //设置topolog模式为分布式，这样topology就可以放到JStorm集群上运行
        try {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());//提交拓扑
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}