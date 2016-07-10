package com.alibaba.middleware.race.jstorm;
import backtype.storm.spout.SpoutOutputCollector;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.ConsumerFactory;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
/*
 * 1.spout对象必须是继承Serializable， 因此要求spout内所有数据结构必须是可序列化的
 * 2.spout可以有构造函数，但构造函数只执行一次，是在提交任务时，创建spout对象,
 * 因此在task分配到具体worker之前的初始化工作可以在此处完成，
 * 一旦完成，初始化的内容将携带到每一个task内
 * （因为提交任务时将spout序列化到文件中去，在worker起来时再将spout从文件中反序列化出来）
 */
public class ConsumerDemoSpout implements IRichSpout,MessageListenerConcurrently  {
    private static Logger LOG = LoggerFactory.getLogger(ConsumerDemoSpout.class);
    
    SpoutOutputCollector collector;
    protected Map conf;
    protected String id;
	protected boolean flowControl;
	protected transient DefaultMQPushConsumer consumer;
	protected transient LinkedBlockingDeque<ConsumerTuple> sendingQueue;
 
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.conf=conf;
		this.collector=collector;
		this.flowControl=true;
		this.sendingQueue = new LinkedBlockingDeque<ConsumerTuple>();
		this.id = context.getThisComponentId() + ":" + context.getThisTaskId();
		
		try {
			consumer = ConsumerFactory.mkInstance(this);
		} catch (Exception e) {
			LOG.error("Failed to create consumerMqPayTopic ", e);
			throw new RuntimeException("Failed to create consumerMqPayTopic" + id, e);
		}

		if (consumer == null) {
			LOG.warn(id
					+ " already exist consumer in current worker, don't need to fetch data ");

			new Thread(new Runnable() {

				@Override
				public void run() {
					while (true) {
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e) {
							break;
						}

						StringBuilder sb = new StringBuilder();
						sb.append("Only on meta consumer can be run on one process,");
						sb.append(" but there are mutliple spout consumes with the same topic@groupid meta, so the second one ");
						sb.append(id).append(" do nothing ");
						LOG.info(sb.toString());
					}
				}
			}).start();
		}
		
		//LOG.info("Successfully init " + id);

	}
	@Override
	public void close() {
		// TODO Auto-generated method stub
		consumer.shutdown();
		
	}
	@Override
	public void activate() {
		// TODO Auto-generated method stub
		if(consumer!=null) {
			consumer.resume();
		}
	}
	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		if (consumer != null) {
			consumer.suspend();
		}
	}
	/*在jstorm中， spout中nextTuple和ack/fail运行在不同的线程中， 
	 * 从而鼓励用户在nextTuple里面执行block的操作，
	 *  原生的storm，nextTuple和ack/fail在同一个线程，
	 *  不允许nextTuple/ack/fail执行任何block的操作，否则就会出现数据超时，
	 *  但带来的问题是，当没有数据时， 整个spout就不停的在空跑，极大的浪费了cpu， 
	 *  因此，jstorm更改了storm的spout设计，鼓励用户block操作（比如从队列中take消息），从而节省cpu。*/
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		ConsumerTuple consumerTuple = null;
		try {
			consumerTuple= sendingQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(consumerTuple == null) {
			return;
		}
		
		sendTuple(consumerTuple);
		
	}
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		//LOG.warn("Shouldn't go this function");
	}
	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		collector.emit(new Values(msgId), msgId);
		//LOG.warn("Shouldn't go this function");
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream(RaceConfig.messageBoltStreamId,new Fields("orderId","consumerTuple"));
		declarer.declareStream(RaceConfig.ratioBoltStreamId, new Fields("paymentMessage"));
		//declarer.declare(new Fields("orderId","consumerTuple"));
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void sendTuple(ConsumerTuple consumerTuple) {
		consumerTuple.updateEmitMs();
		long orderId=consumerTuple.getOrderId();
		//collector.emit(new Values(orderId,consumerTuple),consumerTuple.getCreateMs());
		//collector.emit(new Values(consumerTuple));
		String topic=consumerTuple.getTopic();
		if(topic.equals(RaceConfig.MqPayTopic)) {
			PaymentMessage paymentMessage = consumerTuple.getPaymentMessage();
			collector.emit(RaceConfig.ratioBoltStreamId, new Values(paymentMessage), consumerTuple.getCreateMs());
		}
		collector.emit(RaceConfig.messageBoltStreamId, new Values(orderId,consumerTuple),consumerTuple.getCreateMs());
	}

	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
			ConsumeConcurrentlyContext context) {
		// TODO Auto-generated method stub
		ConsumerTuple consumerTuple = null;
		for (MessageExt msg : msgs) {
			String topic=msg.getTopic();
            byte [] body = msg.getBody();
            if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                //Info: 生产者停止生成数据, 并不意味着马上结束
                //LOG.info("Got the end signal");
                continue;
            }
            PaymentMessage paymentMessage=null;
            OrderMessage orderMessage=null;
            //LOG.info("SPOUT TOPIC:"+topic);
            switch(topic) {
            case RaceConfig.MqPayTopic:
            	paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
            	consumerTuple = new ConsumerTuple(topic, paymentMessage);
            	//LOG.info("paymentMessage:"+paymentMessage);
            	break;
            case RaceConfig.MqTaobaoTradeTopic:
            	orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
            	consumerTuple = new ConsumerTuple(topic, orderMessage);
            	//LOG.info("orderMessage:"+orderMessage);
            	break;
            case RaceConfig.MqTmallTradeTopic:
            	orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
            	consumerTuple = new ConsumerTuple(topic, orderMessage);
            	//LOG.info("orderMessage:"+orderMessage);
            	break;
            default:
            	break;
            }
            
            if(this.flowControl) {
            	try {
					sendingQueue.put(consumerTuple);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            	
            }
            else {
            	sendTuple(consumerTuple);
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
	}

}