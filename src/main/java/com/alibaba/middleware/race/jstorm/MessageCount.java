package com.alibaba.middleware.race.jstorm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairFactory;
import com.alibaba.middleware.race.model.PaymentCount;
import com.taobao.tair.TairManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class MessageCount implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(MessageCount.class);
	/*private static Map<Long, Double> pcPlatform = new ConcurrentHashMap<Long, Double>();
	private static Map<Long, Double> wirelessPlatform = new ConcurrentHashMap<Long, Double>();
	private static Map<Long, Double> tmallMap = new ConcurrentHashMap<Long, Double>();
	private static Map<Long, Double> taobaoMap = new ConcurrentHashMap<Long, Double>();*/
	private static Map<Long, PaymentCount> tmallMap = new ConcurrentHashMap<Long, PaymentCount>();
	private static Map<Long, PaymentCount> taobaoMap = new ConcurrentHashMap<Long, PaymentCount>();
	//private static Map<Long, RatioMessage> platformMap = new ConcurrentHashMap<Long, RatioMessage>();
	private Lock tmallLock = new ReentrantLock();
	private Lock taobaoLock = new ReentrantLock();
	//private Lock platformLock = new ReentrantLock();
	
	private static ExecutorService executorService = Executors.newSingleThreadExecutor();
	private static TairManager tairManager;
	//private static final int namespace=RaceConfig.TairNamespace;
	private static final String tmallPrefix="platformTmall_457160tzhg_";
	private static final String taobaoPrefix="platformTaobao_457160tzhg_";
	
	//private static long startTime=Long.MAX_VALUE;
	//private static long endTime=Long.MIN_VALUE;
	//private static DecimalFormat df = new DecimalFormat("#.00"); 
	private static boolean stopThread=false;
	OutputCollector collector;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		tairManager=TairFactory.tairInstance();
		executorService.execute(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				boolean result;
				while(!stopThread) {
					try {
						Thread.sleep(500);//每隔0.5秒存一次数据
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					for(Map.Entry<Long, PaymentCount> entry:tmallMap.entrySet()) {
						//ResultCode result = tairManager.put(namespace, tmallPrefix+entry.getKey(), entry.getValue().getCurrentTotal());
						result = entry.getValue().writeIntoTair(tmallPrefix, entry.getKey(), tairManager);
						//LOG.info("Tair Input: "+tmallPrefix+ entry.getKey() + entry.getValue());
						/*if (!result)
			                LOG.error("fail input: "+tmallPrefix+ entry.getKey() + entry.getValue());*/
					}
					
					for(Map.Entry<Long, PaymentCount> entry:taobaoMap.entrySet()) {
						//ResultCode result = tairManager.put(namespace, taobaoPrefix+entry.getKey(), entry.getValue().getCurrentTotal());
						result = entry.getValue().writeIntoTair(taobaoPrefix, entry.getKey(), tairManager);
						//LOG.info("Tair Input: "+ taobaoPrefix + entry.getKey() + entry.getValue());
						/*if (!result)
			                LOG.error("fail input: "+ taobaoPrefix + entry.getKey() + entry.getValue());*/
					}
				}
			}
		});
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		long timestamp=(long) input.getValue(0);
		String topicString=(String) input.getValue(1);
		double payAmount=(double) input.getValue(2);
		//short payPlatform= (short) input.getValue(3);
		//Double totalprice=null;
		//LOG.info("INPUTBOLT TOPIC:"+topicString+" TIMESTAMP:"+timestamp);
		switch(topicString) {
		case RaceConfig.MqTaobaoTradeTopic:
			if (!taobaoMap.containsKey(timestamp)) {
				taobaoLock.lock();
				try {
					if (!taobaoMap.containsKey(timestamp)) {
						taobaoMap.put(timestamp, new PaymentCount());
					}
				} catch (Exception e) {
					// TODO: handle exception
				}finally{
					taobaoLock.unlock();
				}
			}
			taobaoMap.get(timestamp).incr(payAmount);
			break;
		case RaceConfig.MqTmallTradeTopic:
			if (!tmallMap.containsKey(timestamp)) {
				tmallLock.lock();
				try {
					if (!tmallMap.containsKey(timestamp)) {
						tmallMap.put(timestamp, new PaymentCount());
					}
				} catch (Exception e) {
					// TODO: handle exception
				}finally{
					tmallLock.unlock();
				}
			}
			tmallMap.get(timestamp).incr(payAmount);
			break;
		}
		/*switch(payPlatform) {
		case 0:
			totalprice=pcPlatform.get(timestamp);
			if (totalprice==null)
				totalprice=(double) 0;
			totalprice+=payAmount;
			pcPlatform.put(timestamp, totalprice);
			break;
		case 1:
			totalprice=wirelessPlatform.get(timestamp);
			if (totalprice==null)
				totalprice=(double) 0;
			totalprice+=payAmount;
			wirelessPlatform.put(timestamp, totalprice);
			break;	
		}*/
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		stopThread=true;
		executorService.shutdown();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}