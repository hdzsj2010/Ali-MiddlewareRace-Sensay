package com.alibaba.middleware.race.model;

public class PaymentCount {
	protected double currentTotal=0;
	synchronized public void incr(double num) {
		currentTotal+=num;
	}
	
	public double getCurrentTotal() {
		return currentTotal;
	}
	

}