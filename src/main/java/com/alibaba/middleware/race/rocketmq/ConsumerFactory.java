package com.alibaba.middleware.race.rocketmq;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

public class ConsumerFactory {
	private static Logger LOG = LoggerFactory.getLogger(ConsumerFactory.class);
   
    public static Map<String, DefaultMQPushConsumer> consumers = new HashMap<String, DefaultMQPushConsumer>();
    
    public static synchronized DefaultMQPushConsumer mkInstance(MessageListenerConcurrently listener)  throws Exception
    {
    	String key = RaceConfig.MetaConsumerGroup;
    	DefaultMQPushConsumer consumer = consumers.get(key);
    	if (consumer != null) {
    		LOG.info("Consumer of " + key + " has been created, don't recreate it ");
    		return null;
    	}
    	
    	consumer = new DefaultMQPushConsumer(key);
    	
    	/**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
    	//consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //在本地搭建好broker后,记得指定nameServer的地址
        //consumer.setNamesrvAddr(RaceConfig.RocketmqNamesrvAddr);
        consumer.subscribe(RaceConfig.MqPayTopic, "*");
        consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
        consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
        consumer.registerMessageListener(listener);
        consumer.start();
        
        consumers.put(key, consumer);
        
        LOG.info("Successfully create " + key + " consumer");
		return consumer;
	}
	
}