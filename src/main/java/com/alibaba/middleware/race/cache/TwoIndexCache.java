package com.alibaba.middleware.race.cache;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiangchao on 2016/7/21.
 */
public class TwoIndexCache {

    public static Map<Integer, TreeMap<Long, Long>> orderIdTwoIndexCache = new ConcurrentHashMap<Integer, TreeMap<Long, Long>>();

    public static Map<Integer, TreeMap<String, Long>> goodIdTwoIndexCache = new ConcurrentHashMap<Integer, TreeMap<String, Long>>();

    public static Map<Integer, TreeMap<String, Long>> buyerIdTwoIndexCache = new ConcurrentHashMap<Integer, TreeMap<String, Long>>();

    public static long findOrderIdOneIndexPosition(long orderId, int index) {
        TreeMap<Long, Long> map = orderIdTwoIndexCache.get(index);

        Entry<Long, Long> entry = map.floorEntry(orderId);
        return entry == null ? 0L : entry.getValue();
    }

    public static long findGoodIdOneIndexPosition(String goodId, int index) {
        TreeMap<String, Long> map = goodIdTwoIndexCache.get(index);

        Entry<String, Long> entry = map.floorEntry(goodId);
        return entry == null ? 0L : entry.getValue();
    }

    public static long findBuyerIdOneIndexPosition(String buyerId,
            long starttime, long endtime, int index) {
        TreeMap<String, Long> map = buyerIdTwoIndexCache.get(index);

        Entry<String, Long> entry = map.floorEntry(buyerId);

        return entry == null ? 0L : entry.getValue();
    }
}
