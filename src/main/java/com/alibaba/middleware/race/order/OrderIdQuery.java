package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.buyer.BuyerQuery;
import com.alibaba.middleware.race.cache.FileNameCache;
import com.alibaba.middleware.race.cache.IndexSizeCache;
import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.constant.IndexConstant;
import com.alibaba.middleware.race.good.GoodQuery;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.KeyValue;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.model.Result;
import com.alibaba.middleware.race.util.RandomAccessFileUtil;

import java.io.*;
import java.util.*;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class OrderIdQuery {

    public static Result findOrder(long orderId, Collection<String> keys) {
        Result result = new Result();
        int hashIndex = (int) (orderId % Config.ORDER_ONE_INDEX_FILE_NUMBER);
        Order order = OrderIdQuery.findByOrderIdAndHashIndex(orderId, hashIndex);

        // 过滤key
        List<String> maybeOrderSearchKeys = new ArrayList<String>();
        List<String> goodSearchKeys = new ArrayList<String>();
        List<String> buyerSearchKeys = new ArrayList<String>();
        if (keys != null) {
            for (String key : keys) {
                if (KeyCache.goodKeyCache.containsKey(key)) {
                    goodSearchKeys.add(key);
                } else if (KeyCache.buyerKeyCache.containsKey(key)) {
                    buyerSearchKeys.add(key);
                } else {
                    maybeOrderSearchKeys.add(key);
                }
            }
        }

        // 特殊查询
        if (order == null) {
            return null;
        }
        if (keys != null && keys.isEmpty()) {
            result.setOrderId(orderId);
            return result;
        }

        // 加入对应买家的所有属性kv
        {
            if (keys == null || buyerSearchKeys.size() > 0) {
                String buyerId = order.getKeyValues().get(IndexConstant.BUYER_ID).getValue();
                Buyer buyer = BuyerQuery.findBuyerById(buyerId);
                if (buyer != null && buyer.getKeyValues() != null) {
                    result.getKeyValues().putAll(buyer.getKeyValues());
                }
            }
        }

        // 加入对应商品的所有属性kv
        {
            if (keys == null || goodSearchKeys.size() > 0) {
                String goodId = order.getKeyValues().get(IndexConstant.GOOD_ID).getValue();
                Good good = GoodQuery.findGoodById(goodId);
                if (good != null && good.getKeyValues() != null) {
                    result.getKeyValues().putAll(good.getKeyValues());
                }
            }

        }

        // 加入对应订单的所有属性kv
        if (keys == null) {
            result.getKeyValues().putAll(order.getKeyValues());
        } else {
            for (String key : maybeOrderSearchKeys) {
                if (order.getKeyValues().containsKey(key)) {
                    result.getKeyValues().put(key, order.getKeyValues().get(key));
                }
            }
        }

        result.setOrderId(orderId);
        return result;
    }

    private static Order findByOrderIdAndHashIndex(long orderId, int hashIndex) {
        Order order = new Order();
        try {
            File indexFile = new File(Config.FIRST_DISK_PATH
                    + FileConstant.SORTED_ORDER_ID_ONE_INDEX_FILE_PREFIX
                    + hashIndex);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "r");

            // 1.查找二级索引
            long position = TwoIndexCache.findOrderIdOneIndexPosition(orderId, hashIndex);

            // 2.查找一级索引
            String oneIndex = null;
            int count = 0;
            long offset = position;
            while ((oneIndex = RandomAccessFileUtil.readLine(indexRaf, offset)) != null) {
                offset += (oneIndex.getBytes().length + 1);
                String[] keyValue = oneIndex.split(":");
                if (orderId == Long.valueOf(keyValue[0])) {
                    break;
                }
                count++;
                if (count >= IndexSizeCache.orderIdIndexRegionSizeMap.get(hashIndex)) {
                    indexRaf.close();
                    return null;
                }
            }
            indexRaf.close();

            // 3.按行读取内容
            String[] keyValue = oneIndex.split(":");
            String srcFile = keyValue[1];
            long pos = Long.valueOf(keyValue[2]);

            File hashFile = new File(FileNameCache.fileNameMap.get(Integer.valueOf(srcFile)));
            RandomAccessFile hashRaf = new RandomAccessFile(hashFile, "r");
            String orderContent = oneIndex = RandomAccessFileUtil.readLine(hashRaf, Long.valueOf(pos));

            // 4.将字符串转成order对象集合
            StringTokenizer stringTokenizer = new StringTokenizer(orderContent, "\t");
            while (stringTokenizer.hasMoreElements()) {
                StringTokenizer kvalue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                String key = kvalue.nextToken();
                String value = kvalue.nextToken();
                KeyValue kv = new KeyValue();
                kv.setKey(key);
                kv.setValue(value);
                order.getKeyValues().put(key, kv);
            }
            order.setId(orderId);
            hashRaf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return order;
    }
}
