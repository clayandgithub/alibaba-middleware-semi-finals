package com.alibaba.middleware.race.cache;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiangchao on 2016/7/21.
 */
public class KeyCache {

    // public static ConcurrentHashMap<String, Integer> orderKeyCache = new ConcurrentHashMap<String, Integer>();

    public static ConcurrentHashMap<String, Integer> buyerKeyCache = new ConcurrentHashMap<String, Integer>();

    public static ConcurrentHashMap<String, Integer> goodKeyCache = new ConcurrentHashMap<String, Integer>();

}
