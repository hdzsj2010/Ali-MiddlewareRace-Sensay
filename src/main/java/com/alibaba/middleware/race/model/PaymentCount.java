package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairManager;

public class PaymentCount {
	protected double currentTotal=0;
	private boolean isChanged = true;
	synchronized public void incr(double num) {
		currentTotal+=num;
		isChanged=true;
	}
	
	public double getCurrentTotal() {
		return currentTotal;
	}
	
	synchronized public boolean writeIntoTair(String predix, long timestamp, TairManager tairManager) {
		if(isChanged) {
			ResultCode result = tairManager.put(RaceConfig.TairNamespace, predix+timestamp, currentTotal);
			if (!result.isSuccess())
				return false;
			isChanged=false;
		}
		return true;	
	}

}