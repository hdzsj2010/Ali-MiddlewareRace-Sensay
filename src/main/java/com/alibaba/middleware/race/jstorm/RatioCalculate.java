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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.Tair.TairFactory;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.model.RatioCount;
import com.alibaba.middleware.race.model.RatioMessage;
import com.taobao.tair.TairManager;

public class RatioCalculate implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(RatioCalculate.class);
	private Lock platformLock = new ReentrantLock();
	//private Lock platformAllLock = new ReentrantLock();
	private static Map<Long, RatioMessage> platformMap = new ConcurrentHashMap<Long, RatioMessage>();
	private static Map<Long, RatioCount> platformAllMap = new ConcurrentHashMap<Long, RatioCount>();
	private static ExecutorService executorService = Executors.newSingleThreadExecutor();
	private static TairManager tairManager;
	private static long startTime=Long.MAX_VALUE;
	private static long endTime=Long.MIN_VALUE;
	private static DecimalFormat df = new DecimalFormat("#.00"); 
	private static boolean stopThread=false;
	OutputCollector collector;
	private static final String ratioPrefix="ratio_457160tzhg_";
	
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
					//double ratio=0;
					//boolean result;
					//LOG.info("startTime is " + startTime +" endTime is " + endTime);
					for(long i = startTime; i<=endTime; i+=60) {
						wirelessAmount+=platformMap.get(i).getWirelessAmount();
						pcAmount+=platformMap.get(i).getPcAmount();
						/*if(pcAmount>0.01) {
							ratio=wirelessAmount/pcAmount;
							result = tairManager.put(RaceConfig.TairNamespace, ratioPrefix +i, df.format(ratio));
							//LOG.info("Tair Input: "+ RaceConfig.prex_ratio + i + ": " + pcAmount + " " + wirelessAmount + " " + df.format(ratio));
							if (!result.isSuccess())
								LOG.error("fail input: "+ ratioPrefix + i + ":" + df.format(ratio));
						}*/
						//7.10改为判断变化再更新
						if(!platformAllMap.containsKey(i)){
							//交易总额初始化
							RatioCount ratioCount = new RatioCount();
							ratioCount.setPcAmountAll(pcAmount);
							ratioCount.setWirelessAll(wirelessAmount);
							platformAllMap.put(i, ratioCount);
							ratioCount.writeIntoTair(ratioPrefix, i, tairManager, df);
							/*if (!result)
								LOG.error("fail input: "+ RaceConfig.prex_ratio + i + ":" + df.format(ratioCount.getRatio()));*/
						}else{
							//当前时分交易总额对比，有更新则写入Tair
							double pcAll = platformAllMap.get(i).getPcAmountAll();
							double wirelessAll = platformAllMap.get(i).getWirelessAmountAll();
							//platformAllLock.lock();
							
							if(pcAll<pcAmount)
								platformAllMap.get(i).setPcAmountAll(pcAmount);
							if(wirelessAll<wirelessAmount)
								platformAllMap.get(i).setWirelessAll(wirelessAmount);
							platformAllMap.get(i).writeIntoTair(ratioPrefix, i, tairManager, df);
							
							/*if (!result)
								LOG.error("fail input: "+ RaceConfig.prex_ratio + i + ":" + df.format(platformAllMap.get(i).getRatio()));*/
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
