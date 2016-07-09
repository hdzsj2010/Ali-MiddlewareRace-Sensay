package com.alibaba.middleware.race.Tair;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.impl.DefaultTairManager;

public class TairFactory {
	private static Logger LOG = LoggerFactory.getLogger(TairFactory.class);
	private static DefaultTairManager tairManager;
    public static DefaultTairManager tairInstance() {
    	List<String> confServers = new ArrayList<String>();
    	//confServers.add(RaceConfig.TairServerAddr);
    	confServers.add(RaceConfig.TairConfigServer);
    	confServers.add(RaceConfig.TairSalveConfigServer);
    	tairManager = new DefaultTairManager();
        tairManager.setConfigServerList(confServers);
        tairManager.setGroupName(RaceConfig.TairGroup);
        tairManager.init();
        return tairManager;
    }
}