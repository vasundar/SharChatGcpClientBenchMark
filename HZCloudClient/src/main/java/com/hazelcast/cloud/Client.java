package com.hazelcast.cloud;

import java.util.Random;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import static com.hazelcast.client.properties.ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN;
import static com.hazelcast.client.properties.ClientProperty.STATISTICS_ENABLED;

/**
 * This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
 * After successful connection, it puts random entries into the map.
 * <p>
 * See: <a href="https://docs.cloud.hazelcast.com/docs/java-client">https://docs.cloud.hazelcast.com/docs/java-client</a>
 */
public class Client {
    public static void main(String[] args) {
        int batch = 100000;
        int threadCount = 50;
        for (int i = 0; i < threadCount; i++) {
            MyThread thread = new MyThread(i * batch);
            thread.setDaemon(true);
            thread.start();
        }
    }
}


class MyThread extends Thread {
    private int value;
    HazelcastInstance client;
    Random random = new Random();
    MyThread(int value) {
        this.value = value;
        ClientConfig config = new ClientConfig();
        config.setProperty(STATISTICS_ENABLED.getName(), "true");
        config.setProperty(HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), "2B6qs5q3NCs8FGvBe6N2jmS3wpfrcGw1w2yGRFK54gKczwbHOk");
        config.setProperty("hazelcast.client.cloud.url", "https://uat.hazelcast.cloud");
        config.setClusterName("twonode");
        client = HazelcastClient.newHazelcastClient(config);
        System.out.println("Connection Successful!  " + Thread.currentThread().getName());
    }
    public void run() {
        IMap<String, String> map = client.getMap("mapTwo");
        long tt = 0;
        long gtt = 0;
        long totalCount = 0;
        long iterCount = 0;

        while(true) {
            int rkey = value + random.nextInt(100_000);
            long stp = System.currentTimeMillis();
            map.put("key-" +rkey, "value-" +rkey);
            long ftp = System.currentTimeMillis();
            long stg = System.currentTimeMillis();
            map.get("key-" + (value +  random.nextInt(100_000)));
            long ftg = System.currentTimeMillis();
            if(iterCount == 1000) {
                iterCount = 0;
                System.out.println("Current Map size : " + map.size());
            }
            totalCount++;
            long diff = ftp - stp;
            tt += diff;
            System.out.println("put average is :" + (float)tt/totalCount);
            long gdiff = ftg - stg;
            gtt += gdiff;
            System.out.println("get average is " + (float)gtt/totalCount);
        }
    }
}

