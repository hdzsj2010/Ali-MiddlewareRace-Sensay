package com.alibaba.middleware.race.model;

import java.io.Serializable;
import java.text.DecimalFormat;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairManager;

public class RatioCount implements Serializable{
	private static final long serialVersionUID = 3L;
	private double ratio=0;
	private double pcAmountAll=0;
	private double wirelessAmountAll=0;
	private boolean isChanged = true;
	
	/*synchronized public void incr(short mark, double number) {
		switch(mark) {
		case 0:
			pcAmountAll=number;
			break;
		case 1:
			wirelessAmountAll=number;
			break;
		}
		//isChanged = true;
	}*/
	
	synchronized public void setPcAmountAll(double pcAll){
		this.pcAmountAll = pcAll;
		isChanged = true;
	}
	
	synchronized public void setWirelessAll(double wirelessAll){
		this.wirelessAmountAll = wirelessAll;
		isChanged = true;
	}
	
	public double getPcAmountAll() {
		return pcAmountAll;
	}
	
	public double getWirelessAmountAll() {
		return wirelessAmountAll;
	}
	
	public double getRatio() {
		return ratio;
	}
	
	synchronized public void writeIntoTair(String ratioPrefix, long timestamp, TairManager tairManager, DecimalFormat df){
		if(isChanged && pcAmountAll>0.01){
			ratio = wirelessAmountAll/pcAmountAll;
			ResultCode result = tairManager.put(RaceConfig.TairNamespace, ratioPrefix+timestamp, df.format(ratio));
			if(!result.isSuccess())
				return;
			isChanged = false;
		}
		return;
	}
}
