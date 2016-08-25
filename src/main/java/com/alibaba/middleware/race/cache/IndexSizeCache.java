/**
 * IndexSizeCache.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wangweiwei
 *
 */
public class IndexSizeCache {
    public static Map<Integer, Integer> goodIdIndexRegionSizeMap = new ConcurrentHashMap<Integer, Integer>();

    public static Map<Integer, Integer> buyerIdIndexRegionSizeMap = new ConcurrentHashMap<Integer, Integer>();

    public static Map<Integer, Integer> orderIdIndexRegionSizeMap = new ConcurrentHashMap<Integer, Integer>();
}
