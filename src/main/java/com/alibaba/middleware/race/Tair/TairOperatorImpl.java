package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {

    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
    }

    public boolean write(Serializable key, Serializable value) {
        return false;
    }

    public Object get(Serializable key) {
        return null;
    }

    public boolean remove(Serializable key) {
        return false;
    }

    public void close(){
    }

    //天猫的分钟交易额写入tair
    public static void main(String [] args) throws Exception {
        /*TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //假设这是付款时间
        Long millisTime = System.currentTimeMillis();
        //由于整分时间戳是10位数，所以需要转换成整分时间戳
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        //假设这一分钟的交易额是100;
        Double money = 100.0;
        //写入tair
        tairOperator.write(RaceConfig.prex_tmall + minuteTime, money);*/
    	
    	// 自己的程序:创建config server列表
        List<String> confServers = new ArrayList<String>();
        confServers.add("10.109.247.166:5198");
        // confServers.add("10.10.7.144:51980"); // 可选

        // 创建客户端实例
        DefaultTairManager tairManager = new DefaultTairManager();
        tairManager.setConfigServerList(confServers);


        // 设置组名
        tairManager.setGroupName("group_1");
        // 初始化客户端
        tairManager.init();

        // put 10 items
        for (int i = 0; i < 10; i++) {
            // 第一个参数是namespace，第二个是key，第三是value，第四个是版本，第五个是有效时间
            ResultCode result = tairManager.put(0, "k54" + i, 1.23, 0);
            System.err.println("put k" + i + ":" + result.isSuccess());
            if (!result.isSuccess())
                break;
        }

        for (int i = 0; i < 10; i++) {
        	// 第一个参数是namespace，第二个是key，第三是value，第四个是版本，第五个是有效时间
            Result<DataEntry> result = tairManager.get(0, "k54" + i);
            if (result.isSuccess()) {
                DataEntry entry = result.getValue();
                if (entry != null) {
                	// 数据存在
                    System.err.println("value is " + entry.getValue().toString());
                } else {
                	// 数据不存在
                    System.err.println("this key doesn't exist.");
                }
            } else {
            	// 异常处理
                System.out.println(result.getRc().getMessage());
            }
        }
        tairManager.close();
        //System.out.println("????");
        System.exit(0);
    }
}
