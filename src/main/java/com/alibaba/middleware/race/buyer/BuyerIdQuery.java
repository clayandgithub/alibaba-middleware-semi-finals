package com.alibaba.middleware.race.buyer;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.cache.IndexSizeCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.constant.IndexConstant;
import com.alibaba.middleware.race.good.GoodQuery;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.util.RandomAccessFileUtil;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class BuyerIdQuery {

    public static Iterator<Result> findOrdersByBuyer(long startTime, long endTime, String buyerid) {
        List<Result> results = new ArrayList<Result>();
        int hashIndex = (int) (Math.abs(buyerid.hashCode()) % Config.ORDER_ONE_INDEX_FILE_NUMBER);

        Buyer buyer = BuyerQuery.findBuyerById(buyerid);
        if (buyer == null) {
            return results.iterator();
        }

        // 获取buyerid的所有符合条件的订单信息
        List<Order> orders = BuyerIdQuery.findByBuyerIdAndIndexFileIndex(buyerid, startTime, endTime, hashIndex);
        if (orders == null || orders.size() == 0) {
            return results.iterator();
        }

        for (Order order : orders) {
            Result result = new Result();
            // 加入对应买家的所有属性kv
            if (buyer != null && buyer.getKeyValues() != null) {
                result.getKeyValues().putAll(buyer.getKeyValues());
            }
            // 加入对应商品的所有属性kv
            Good good = GoodQuery.findGoodById(order.getKeyValues().get(IndexConstant.GOOD_ID).getValue());

            if (good != null && good.getKeyValues() != null) {
                result.getKeyValues().putAll(good.getKeyValues());
            }
            // 加入订单信息的所有属性kv
            result.getKeyValues().putAll(order.getKeyValues());
            result.setOrderId(order.getId());
            results.add(result);
        }
        return results.iterator();
    }

    private static List<Order> findByBuyerIdAndIndexFileIndex(String buyerId, long starttime, long endtime, int index) {
        if (buyerId == null || buyerId.isEmpty())
            return null;
        List<Order> orders = new ArrayList<Order>();
        try {
            RandomAccessFile indexRaf = new RandomAccessFile(
                    Config.SECOND_DISK_PATH
                            + FileConstant.SORTED_BUYER_ID_ONE_INDEX_FILE_PREFIX
                            + index, "r");

            // 1.查找二·级索引
            long position = TwoIndexCache.findBuyerIdOneIndexPosition(buyerId, starttime, endtime, index);
            System.out.println(position);
            // 2.查找一级索引
            int count = 0;
            String oneIndex = null;
            long offset = position;
            while ((oneIndex = RandomAccessFileUtil.readLine(indexRaf, offset)) != null) {
                offset += (oneIndex.getBytes().length + 1);
                String[] keyValue = oneIndex.split(":");
                if (buyerId.equals(keyValue[0])) {
                    break;
                }
                count++;
                if (count >= IndexSizeCache.buyerIdIndexRegionSizeMap.get(index)) {
                    indexRaf.close();
                    return null;
                }
            }
            if (oneIndex == null) {
                indexRaf.close();
                return null;
            }


            // 3.处理一级索引逻辑，计算最终读取的起始位置和length
            String[] keyValue = oneIndex.split(":");
            String[] positionKvs = keyValue[2].split("\\|");
            long startPosition = Long.valueOf(keyValue[1]);
            int length = 0;

            for (String pos : positionKvs) {
                String[] posKv = pos.split("_");
                Long createTime = Long.valueOf(posKv[0]);
                if (createTime >= endtime) {
                    startPosition += Integer.valueOf(posKv[1]);
                    continue;
                }
                if (createTime < starttime) {
                    break;
                }
                length += Integer.valueOf(posKv[1]);
            }

            // 4.按块读取内容
            File rankFile = new File(Config.SECOND_DISK_PATH
                                     + FileConstant.SORTED_BUYER_ID_HASH_FILE_PREFIX + index);
            RandomAccessFile hashRaf = new RandomAccessFile(rankFile, "r");
            hashRaf.seek(startPosition);

            byte[] bytes = new byte[length];
            hashRaf.read(bytes, 0, length);
            String orderStrs = new String(bytes);
            StringTokenizer stringTokenizer = new StringTokenizer(orderStrs, "\n");
            while (stringTokenizer.hasMoreElements()) {
                Order order = new Order();
                StringTokenizer orderStringTokenizer = new StringTokenizer(stringTokenizer.nextToken(), "\t");
                while (orderStringTokenizer.hasMoreElements()) {
                    StringTokenizer strs = new StringTokenizer(orderStringTokenizer.nextToken(), ":");
                    String key = strs.nextToken();
                    String value = strs.nextToken();
                    KeyValue kv = new KeyValue();
                    kv.setKey(key);
                    kv.setValue(value);
                    order.getKeyValues().put(key, kv);
                }
                if (order.getKeyValues().get("orderid").getValue() != null
                    && NumberUtils.isNumber(order.getKeyValues().get("orderid").getValue())) {
                    order.setId(Long.valueOf(order.getKeyValues().get("orderid").getValue()));

                }
                orders.add(order);
            }
            indexRaf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return orders;
    }
}
