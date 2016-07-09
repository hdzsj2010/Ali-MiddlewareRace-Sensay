package com.alibaba.middleware.race.jstorm;

import java.io.Serializable;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

public class ConsumerTuple implements Serializable {
	private static final long serialVersionUID = 1L;
	protected PaymentMessage paymentMessage;
	protected OrderMessage orderMessage;
	protected String topic;
	protected long createMs;
	protected long emitMs;
	protected long orderId;
	
	public ConsumerTuple() {}
	
	public ConsumerTuple(String topic, PaymentMessage message) {
		this.topic=topic;
		this.paymentMessage=message;
		this.orderMessage=null;
		this.createMs = System.currentTimeMillis();
		this.orderId = message.getOrderId();
	}
	
	public ConsumerTuple(String topic, OrderMessage message) {
		this.topic=topic;
		this.paymentMessage=null;
		this.orderMessage=message;
		this.createMs = System.currentTimeMillis();
		this.orderId = message.getOrderId();
	}
	
	public long getOrderId() {
		return orderId;
	}
	
	public String getTopic() {
		return topic;
	}
	
	public OrderMessage getOrderMessage() {
		return orderMessage;
	}
	
	public PaymentMessage getPaymentMessage() {
		return paymentMessage;
	}
	
	public long getCreateMs() {
		return createMs;
	}

	public long getEmitMs() {
		return emitMs;
	}

	public void updateEmitMs() {
		this.emitMs = System.currentTimeMillis();
	}
	
}