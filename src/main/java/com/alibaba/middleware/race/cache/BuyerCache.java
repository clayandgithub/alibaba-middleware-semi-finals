package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.constant.IndexConstant;
import com.alibaba.middleware.race.model.Buyer;
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
public class BuyerCache extends Thread {
    public static Map<String, Buyer> buyerMap = new ConcurrentHashMap<String, Buyer>();
    private Collection<String> buyerFiles;
    private CountDownLatch countDownLatch;

    public BuyerCache(Collection<String> buyerFiles,
            CountDownLatch countDownLatch) {
        this.buyerFiles = buyerFiles;
        this.countDownLatch = countDownLatch;
    }

    // 读取所有商品文件，按照商品号hash到多个小文件中, 生成到第一块磁盘中
    public void cacheBuyer() {
        System.gc();
        try {
            for (String buyerFile : buyerFiles) {
                FileInputStream buyer_records = new FileInputStream(buyerFile);
                BufferedReader buyer_br = new BufferedReader(
                        new InputStreamReader(buyer_records));

                String str = null;
                int cacheNum = 0;
                while ((str = buyer_br.readLine()) != null) {
                    if (cacheNum >= 500000) {
                        buyer_br.close();
                        return;
                    }
                    Buyer buyer = new Buyer();
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
                        buyer.getKeyValues().put(key, kv);
                    }
                    buyer.setId(buyer.getKeyValues().get(IndexConstant.BUYER_ID).getValue());
                    BuyerCache.buyerMap.put(buyer.getId(), buyer);
                    cacheNum++;
                }
                buyer_br.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        if (countDownLatch != null) {
            try {
                countDownLatch.await(); // 等待构建索引的所有任务的结束
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("cache buyer  start~");
        cacheBuyer();
        System.out.println("cache buyer  end~");
    }

}
