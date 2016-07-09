package com.alibaba.middleware.race.jstorm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.utils.Pair;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

public class MessageTagsDeal implements IRichBolt {
	//private String dateformate="dd/MM/yyyy HH:mm:ss";
	private static Logger LOG = LoggerFactory.getLogger(MessageTagsDeal.class);
	//private static Map<String, Tuple> paymentCache = new ConcurrentHashMap<String, Tuple>();
	private static LinkedBlockingDeque<Tuple> paymentCache=new LinkedBlockingDeque<Tuple>();
	//orderID,<remainPrice,topic>
	private static Map<Long, Pair<Double, String>> orderCache = new ConcurrentHashMap<Long, Pair<Double,String>>();
	private static boolean stopThread=false;
	OutputCollector collector;
    
    
    private void sendData(long orderId, PaymentMessage paymentMessage, Tuple input){
    	String getTopic = orderCache.get(orderId).getSecond();
		Double remainPrice=orderCache.get(orderId).getFirst()-paymentMessage.getPayAmount();
		if (remainPrice<0.01) {
			orderCache.remove(orderId);
		}
		else {
			orderCache.get(orderId).setFirst(remainPrice);
		}
		
		long timestamp = (paymentMessage.getCreateTime() / 1000 / 60) * 60;
		//LOG.info("MessageTagsDeal==> timestamp:" + timestamp + " getTopic:" +getTopic + " payAmount:" +paymentMessage.getPayAmount() + " payPlatform:" +paymentMessage.getPayPlatform());
		collector.emit(input,new Values(timestamp, getTopic,paymentMessage.getPayAmount()));
    }
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		ExecutorService executorService = Executors.newCachedThreadPool();
		executorService.execute(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				long orderId;
				Tuple input = null;
				PaymentMessage paymentMessage = null;
				while(!stopThread) {
					/*for(Map.Entry<String, Tuple> entry:paymentCache.entrySet()) {
						String key = entry.getKey();
						long orderId = Long.valueOf(key.split(",")[0]);
						//LOG.info("RUNThread:"+orderId);
						//用paymentCache缓存支付信息，当对应的订单信息产生时进行计算
						if(orderCache.containsKey(orderId)) {
							//LOG.info("RUNThread output");
							Tuple input = entry.getValue();
							paymentCache.remove(key);
							PaymentMessage paymentMessage=((ConsumerTuple)input.getValue(1)).getPaymentMessage();
							sendData(orderId, paymentMessage, input);
						}
					}*/
					try {
						input = paymentCache.take();
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					if(input!=null) {
						paymentMessage=((ConsumerTuple)input.getValue(1)).getPaymentMessage();
						orderId=paymentMessage.getOrderId();
						if(orderCache.containsKey(orderId)) {
							sendData(orderId, paymentMessage, input);
						}
						else {
							try {
								paymentCache.put(input);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}
			}
		});
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		long orderId=(long) input.getValue(0);
		ConsumerTuple consumerTuple=(ConsumerTuple)input.getValue(1);
		
		String topic=consumerTuple.getTopic();
		OrderMessage orderMessage=null;
		PaymentMessage paymentMessage=null;
		//LOG.info("BOLT Topic:"+topic);
		switch(topic) {
		case RaceConfig.MqPayTopic:
			paymentMessage=consumerTuple.getPaymentMessage();
			//Long orderId=paymentMessage.getOrderId();
			if (orderCache.containsKey(orderId)) {
				//LOG.info("Start sendData");
				sendData(orderId, paymentMessage, input);
			}
			else {
				//LOG.info("Input paymentCache:"+orderId);
				//重新构造key，防止未处理的支付信息被覆盖
				//String keyString=orderId + "," + paymentMessage.getCreateTime();
				//String keyString=consumerTuple.getPaymentMessageKey();
				try {
					paymentCache.put(input);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			break;
		case RaceConfig.MqTaobaoTradeTopic:
			orderMessage=consumerTuple.getOrderMessage();
			orderCache.put(orderMessage.getOrderId(), new Pair<Double, String>(orderMessage.getTotalPrice(), topic));
			break;
		case RaceConfig.MqTmallTradeTopic:
			orderMessage=consumerTuple.getOrderMessage();
			orderCache.put(orderMessage.getOrderId(), new Pair<Double, String>(orderMessage.getTotalPrice(), topic));
			break;
		default:
        	break;
		}
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		stopThread=true;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("timestamp","topic","price"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
