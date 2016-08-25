package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.constant.IndexConstant;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.KeyValue;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/8/1.
 */
public class GoodCache extends Thread {
    public static Map<String, Good> goodMap = new ConcurrentHashMap<String, Good>();

    private Collection<String> goodFiles;
    private CountDownLatch countDownLatch;

    public GoodCache(Collection<String> goodFiles, CountDownLatch countDownLatch) {
        this.goodFiles = goodFiles;
        this.countDownLatch = countDownLatch;
    }

    // 读取所有商品文件，按照商品号hash到多个小文件中, 生成到第一块磁盘中
    public void cacheGood() {

        try {
            for (String goodFile : goodFiles) {
                FileInputStream good_records = new FileInputStream(goodFile);
                BufferedReader good_br = new BufferedReader(
                        new InputStreamReader(good_records));

                String str = null;
                int cacheNum = 0;
                while ((str = good_br.readLine()) != null) {
                    if (cacheNum >= 1000000) {
                        good_br.close();
                        return;
                    }
                    Good good = new Good();
                    StringTokenizer stringTokenizer = new StringTokenizer(str,
                            "\t");
                    while (stringTokenizer.hasMoreElements()) {
                        StringTokenizer keyValue = new StringTokenizer(
                                stringTokenizer.nextToken(), ":");
                        String key = keyValue.nextToken();
                        String value = keyValue.nextToken();
                        KeyValue kv = new KeyValue();
                        kv.setKey(key);
                        kv.setValue(value);
                        good.getKeyValues().put(key, kv);
                    }
                    good.setId(good.getKeyValues().get(IndexConstant.GOOD_ID).getValue());
                    GoodCache.goodMap.put(good.getId(), good);
                    cacheNum++;
                }
                good_br.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        if (countDownLatch != null) {
            try {
                countDownLatch.await(); // 等待上一个任务的结束
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("cache good  start~");
        cacheGood();
        System.out.println("cache good  end~");
    }
}
