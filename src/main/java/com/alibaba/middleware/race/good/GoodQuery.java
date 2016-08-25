package com.alibaba.middleware.race.good;

import com.alibaba.middleware.race.cache.FileNameCache;
import com.alibaba.middleware.race.cache.OneIndexCache;
import com.alibaba.middleware.race.model.FilePosition;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.KeyValue;
import com.alibaba.middleware.race.util.RandomAccessFileUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class GoodQuery {
    public static Good findGoodById(String goodId) {
        if (goodId == null || goodId.isEmpty()) {
            return null;
        }

        Good good = new Good();

        // 1.查找索引
        FilePosition positionInfo = null;
        if (!OneIndexCache.goodOneIndexCache.containsKey(goodId)) {
            return null;
        } else {
            positionInfo = OneIndexCache.goodOneIndexCache.get(goodId);
        }
        RandomAccessFile hashRaf = null;
        try {
            // 2.按行读取内容
            File rankFile = new File(FileNameCache.fileNameMap.get(positionInfo.getFileNum()));
            hashRaf = new RandomAccessFile(rankFile, "r");

            long offset = positionInfo.getPosition();
            String oneIndex = RandomAccessFileUtil.readLine(hashRaf, offset);
            if (oneIndex == null) {
                return null;
            }

            // 3.将字符串转成good对象
            String[] keyValues = oneIndex.split("\t");
            for (int i = 0; i < keyValues.length; i++) {
                String[] strs = keyValues[i].split(":");
                KeyValue kv = new KeyValue();
                kv.setKey(strs[0]);
                kv.setValue(strs[1]);
                good.getKeyValues().put(strs[0], kv);
            }
            good.setId(goodId);
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
        return good;
    }

}
