package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";


    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public final static String JstormTopologyName = "457160tzhg";
    public final static String MetaConsumerGroup = "457160tzhg";
    public final static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public final static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public final static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 37818;
    
    public final static String ratioBoltStreamId = "stream_ratio";
    public final static String messageBoltStreamId = "stream_message";
}
