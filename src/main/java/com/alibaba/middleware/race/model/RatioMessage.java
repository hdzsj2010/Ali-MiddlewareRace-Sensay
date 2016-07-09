package com.alibaba.middleware.race.model;

import java.io.Serializable;

public class RatioMessage implements Serializable {
	private static final long serialVersionUID = 2L;
	private double pcAmount=0;
	private double wirelessAmount=0;
	
	synchronized public void incr(short mark, double number) {
		switch(mark) {
		case 0:
			pcAmount+=number;
			break;
		case 1:
			wirelessAmount+=number;
			break;
		}
	}
	
	public double getPcAmount() {
		return pcAmount;
	}
	
	public double getWirelessAmount() {
		return wirelessAmount;
	}
}
