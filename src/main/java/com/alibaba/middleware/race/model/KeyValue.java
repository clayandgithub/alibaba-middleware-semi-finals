package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.OrderSystem;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class KeyValue implements OrderSystem.KeyValue {

    private String key;

    private String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String key() {
        return getKey();
    }

    @Override
    public String valueAsString() {
        return getValue();
    }

    @Override
    public long valueAsLong() throws OrderSystem.TypeException {
        return Long.valueOf(value);
    }

    @Override
    public double valueAsDouble() throws OrderSystem.TypeException {
        return Double.valueOf(getValue());
    }

    @Override
    public boolean valueAsBoolean() throws OrderSystem.TypeException {
        if ("true".equals(getValue())) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "KeyValue{" + "key='" + key + '\'' + ", value='" + value + '\''
                + '}';
    }
}
