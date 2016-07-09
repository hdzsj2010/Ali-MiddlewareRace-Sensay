package com.alibaba.middleware.race.jstorm;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.model.RatioMessage;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class RatioCalculate implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(RatioCalculate.class);
	private Lock platformLock = new ReentrantLock();
	private static Map<Long, RatioMessage> platformMap = new ConcurrentHashMap<Long, RatioMessage>();
	private static ExecutorService executorService = Executors.newSingleThreadExecutor();
	private static TairManager tairManager;
	private static long startTime=Long.MAX_VALUE;
	private static long endTime=Long.MIN_VALUE;
	private static DecimalFormat df = new DecimalFormat("#.00"); 
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
				while(!stopThread) {
					try {
						Thread.sleep(200); 
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					List<Long> platformMapList=new ArrayList<Long>(platformMap.keySet());
					if(!platformMapList.isEmpty()) {
						startTime=(long) Collections.min(platformMapList);
						endTime=(long) Collections.max(platformMapList);
					}
					double wirelessAmount=0;
					double pcAmount=0;
					double ratio=0;
					ResultCode result;
					//LOG.info("startTime is " + startTime +" endTime is " + endTime);
					for(long i = startTime; i<=endTime; i+=60) {
						wirelessAmount+=platformMap.get(i).getWirelessAmount();
						pcAmount+=platformMap.get(i).getPcAmount();
						if(pcAmount>0.01) {
							ratio=wirelessAmount/pcAmount;
							result = tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_ratio +i, df.format(ratio));
							//LOG.info("Tair Input: "+ RaceConfig.prex_ratio + i + ": " + pcAmount + " " + wirelessAmount + " " + df.format(ratio));
							if (!result.isSuccess())
								LOG.error("fail input: "+ RaceConfig.prex_ratio + i + ":" + df.format(ratio));
						}
					}					
				}			
			}
		});
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		PaymentMessage paymentMessage=(PaymentMessage) input.getValue(0);
		long timestamp=(paymentMessage.getCreateTime() / 1000 / 60) * 60;
		double payAmount=paymentMessage.getPayAmount();
		short payPlatform= paymentMessage.getPayPlatform();
		if(!platformMap.containsKey(timestamp)) {
			platformLock.lock();
			try {
				if(!platformMap.containsKey(timestamp)) {
					platformMap.put(timestamp, new RatioMessage());
				}
			}catch (Exception e) {
	            // TODO: handle exception
	        }finally {
	        	platformLock.unlock();
	        }				
		}
		//LOG.info("INCRTIMESTAMP"+timestamp+" payPlatform:"+payPlatform + " payAmount:" +payAmount);
		platformMap.get(timestamp).incr(payPlatform, payAmount);
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		stopThread=true;
		executorService.shutdown();
		//LOG.info("ratioCalculateInteger:"+ratioCalculateInteger.get());
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
