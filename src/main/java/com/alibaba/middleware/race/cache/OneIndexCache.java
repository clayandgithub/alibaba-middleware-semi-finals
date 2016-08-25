package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.model.FilePosition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiangchao on 2016/7/24.
 */
public class OneIndexCache {

    public static Map<String, FilePosition> buyerOneIndexCache = new ConcurrentHashMap<String, FilePosition>();

    public static Map<String, FilePosition> goodOneIndexCache = new ConcurrentHashMap<String, FilePosition>();
}
