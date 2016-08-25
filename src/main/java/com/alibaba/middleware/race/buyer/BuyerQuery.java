package com.alibaba.middleware.race.buyer;

import com.alibaba.middleware.race.cache.*;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.FilePosition;
import com.alibaba.middleware.race.model.KeyValue;
import com.alibaba.middleware.race.util.RandomAccessFileUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class BuyerQuery {
    public static Buyer findBuyerById(String buyerId) {
        if (buyerId == null || buyerId.isEmpty()) {
            return null;
        }

        Buyer buyer = BuyerCache.buyerMap.get(buyerId);
        if (buyer != null) {
            return buyer;
        }
        buyer = new Buyer();

        // 1.查找索引
        FilePosition positionInfo = null;
        if (!OneIndexCache.buyerOneIndexCache.containsKey(buyerId)) {
            return null;
        } else {
            positionInfo = OneIndexCache.buyerOneIndexCache.get(buyerId);
        }
        RandomAccessFile hashRaf = null;
        try {
            File rankFile = new File(FileNameCache.fileNameMap.get(positionInfo.getFileNum()));
            hashRaf = new RandomAccessFile(rankFile, "r");

            // 2.按行读取内容
            long offset = positionInfo.getPosition();
            String oneIndex = RandomAccessFileUtil.readLine(hashRaf, offset);
            if (oneIndex == null)
                return null;

            // 3.将字符串转成buyer对象
            String[] keyValues = oneIndex.split("\t");
            for (int i = 0; i < keyValues.length; i++) {
                String[] strs = keyValues[i].split(":");
                KeyValue kv = new KeyValue();
                kv.setKey(strs[0]);
                kv.setValue(strs[1]);
                buyer.getKeyValues().put(strs[0], kv);
            }
            buyer.setId(buyerId);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (hashRaf != null) {
                try {
                    hashRaf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return buyer;
    }

}
