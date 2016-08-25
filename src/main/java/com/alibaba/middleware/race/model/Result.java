package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.OrderSystem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class Result implements OrderSystem.Result {

    private Long orderId;

    Map<String, KeyValue> keyValues = new HashMap<String, KeyValue>();

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Map<String, KeyValue> getKeyValues() {
        return keyValues;
    }

    public void setKeyValues(Map<String, KeyValue> keyValues) {
        this.keyValues = keyValues;
    }

    public void remove(String key) {
        this.keyValues.remove(key);
    }

    @Override
    public KeyValue get(String key) {
        return keyValues.get(key);
    }

    @Override
    public KeyValue[] getAll() {
        List<KeyValue> keyValueList = (List<KeyValue>) keyValues.values();
        return (KeyValue[]) keyValueList.toArray();
    }

    @Override
    public long orderId() {
        return orderId;
    }

    @Override
    public String toString() {
        return "Result{" + "orderId=" + orderId + ", keyValues=" + keyValues
                + '}';
    }
}
