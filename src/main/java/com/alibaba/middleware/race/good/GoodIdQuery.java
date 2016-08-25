package com.alibaba.middleware.race.good;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.buyer.BuyerQuery;
import com.alibaba.middleware.race.cache.IndexSizeCache;
import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.KeyValue;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.model.Result;
import com.alibaba.middleware.race.util.RandomAccessFileUtil;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class GoodIdQuery {
    private static List<Order> findByGoodId(String goodId, int index) {
        if (goodId == null)
            return null;
        List<Order> orders = new ArrayList<Order>();
        try {

            File indexFile = new File(Config.THIRD_DISK_PATH + FileConstant.SORTED_GOOD_ID_ONE_INDEX_FILE_PREFIX + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "r");

            // 1.查找二·级索引
            long position = TwoIndexCache.findGoodIdOneIndexPosition(goodId, index);

            // 2.查找一级索引
            int count = 0;
            String oneIndex = null;
            long offset = position;
            while ((oneIndex = RandomAccessFileUtil.readLine(indexRaf, offset)) != null) {
                offset += (oneIndex.getBytes().length + 1);
                String[] keyValue = oneIndex.split(":");
                if (goodId.equals(keyValue[0])) {
                    break;
                }
                count++;
                if (count >= IndexSizeCache.goodIdIndexRegionSizeMap.get(index)) {
                    indexRaf.close();
                    return null;
                }
            }
            if (oneIndex == null) {
                indexRaf.close();
                return null;
            }

            // 3.按块读取内容
            String[] keyValue = oneIndex.split(":");
            String pos = keyValue[1];
            int length = Integer.valueOf(keyValue[2]);
            File rankFile = new File(Config.THIRD_DISK_PATH + FileConstant.SORTED_GOOD_ID_HASH_FILE_PREFIX + index);
            RandomAccessFile hashRaf = new RandomAccessFile(rankFile, "r");
            hashRaf.seek(Long.valueOf(pos));

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

            hashRaf.close();
            indexRaf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return orders;
    }

    public static int findOrderNumberByGoodKey(String goodId, int index) {
        if (goodId == null)
            return 0;
        try {
            File indexFile = new File(Config.THIRD_DISK_PATH
                    + FileConstant.SORTED_GOOD_ID_ONE_INDEX_FILE_PREFIX + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "r");

            // 1.查找二·级索引
            long position = TwoIndexCache.findGoodIdOneIndexPosition(goodId, index);

            // 2.查找一级索引
            int count = 0;
            String oneIndex = null;
            long offset = position;
            while ((oneIndex = RandomAccessFileUtil.readLine(indexRaf, offset)) != null) {
                offset += (oneIndex.getBytes().length + 1);
                String[] keyValue = oneIndex.split(":");
                if (goodId.equals(keyValue[0])) {
                    break;
                }
                count++;
                if (count >= IndexSizeCache.goodIdIndexRegionSizeMap.get(index)) {
                    indexRaf.close();
                    return 0;
                }
            }
            if (oneIndex == null) {
                indexRaf.close();
                return 0;
            }
            indexRaf.close();
            String[] keyValue = oneIndex.split(":");

            return Integer.valueOf(keyValue[3]);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static Iterator<Result> findOrdersByGood(String salerid, String goodid, Collection<String> keys) {
        List<Result> results = new ArrayList<Result>();
        if (goodid == null) {
            return results.iterator();
        }

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

        int hashIndex = (int) (Math.abs(goodid.hashCode()) % Config.ORDER_ONE_INDEX_FILE_NUMBER);

        Good good = null;
        if (keys == null || goodSearchKeys.size() > 0) {
            // 加入对应商品的所有属性kv
            good = GoodQuery.findGoodById(goodid);
            if (good == null)
                return results.iterator();

        }

        // 获取goodid的所有订单信息
        List<Order> orders = GoodIdQuery.findByGoodId(goodid, hashIndex);
        if (orders == null || orders.size() == 0) {
            return results.iterator();
        }
        if (keys != null && keys.size() == 0) {
            for (Order order : orders) {
                Result result = new Result();
                result.setOrderId(order.getId());
                results.add(result);
            }
            // 对所求结果按照交易订单从小到大排序
            return results.iterator();
        }

        for (Order order : orders) {
            Result result = new Result();
            if (keys == null || buyerSearchKeys.size() > 0) {
                // 加入对应买家的所有属性kv
                Buyer buyer = BuyerQuery.findBuyerById(order.getKeyValues().get("buyerid").getValue());

                if (buyer != null && buyer.getKeyValues() != null) {
                    result.getKeyValues().putAll(buyer.getKeyValues());
                }
            }

            if (good != null) {
                result.getKeyValues().putAll(good.getKeyValues());
            }

            // 加入订单信息的所有属性kv
            if (keys == null) {
                result.getKeyValues().putAll(order.getKeyValues());
            } else {
                for (String key : maybeOrderSearchKeys) {
                    if (order.getKeyValues().containsKey(key)) {
                        result.getKeyValues().put(key, order.getKeyValues().get(key));
                    }
                }
            }

            result.setOrderId(order.getId());
            results.add(result);
        }
        return results.iterator();
    }

    public static OrderSystem.KeyValue sumValuesByGood(String goodid, String key) {
        if (goodid == null || key == null)
            return null;
        KeyValue keyValue = new KeyValue();
        int hashIndex = (int) (Math.abs(goodid.hashCode()) % Config.ORDER_ONE_INDEX_FILE_NUMBER);
        double value = 0;
        long longValue = 0;
        // flag=0表示Long类型，1表示Double类型
        int flag = 0;

        if (KeyCache.goodKeyCache.containsKey(key)) {
            // 加入对应商品的所有属性kv
            int num = GoodIdQuery.findOrderNumberByGoodKey(goodid, hashIndex);
            Good good = GoodQuery.findGoodById(goodid);

            if (good == null)
                return null;
            if (good.getKeyValues().containsKey(key)) {
                String str = good.getKeyValues().get(key).getValue();
                if (flag == 0 && str.contains(".")) {
                    flag = 1;
                }
                if (GoodIdQuery.isNumeric(str)) {
                    if (flag == 0) {
                        longValue = num * Long.valueOf(str);
                        keyValue.setKey(key);
                        keyValue.setValue(String.valueOf(longValue));
                    } else {
                        value = num * Double.valueOf(str);
                        keyValue.setKey(key);
                        keyValue.setValue(String.valueOf(value));
                    }
                    return keyValue;
                }
                return null;
            } else {
                return null;
            }
        }

        List<Order> orders = GoodIdQuery.findByGoodId(goodid, hashIndex);
        if (orders == null || orders.size() == 0)
            return null;
        int count = 0;

        for (Order order : orders) {
            // 加入订单信息的所有属性kv
            if (order.getKeyValues().containsKey(key)) {
                String str = order.getKeyValues().get(key).getValue();
                if (flag == 0 && str.contains(".")) {
                    flag = 1;
                }
                if (NumberUtils.isNumber(str)) {
                    if (flag == 0) {
                        longValue += Long.valueOf(str);
                        value += Double.valueOf(str);
                    } else {
                        value += Double.valueOf(str);
                    }
                    count++;
                    continue;
                }
                return null;
            }

            // 加入对应买家的所有属性kv
            if (KeyCache.buyerKeyCache.containsKey(key)) {
                Buyer buyer = BuyerQuery.findBuyerById(order.getKeyValues().get("buyerid").getValue());
                if (buyer.getKeyValues().containsKey(key)) {
                    String str = buyer.getKeyValues().get(key).getValue();
                    if (flag == 0 && str.contains(".")) {
                        flag = 1;
                    }
                    if (NumberUtils.isNumber(str)) {
                        if (flag == 0) {
                            longValue += Long.valueOf(str);
                            value += Double.valueOf(str);
                        } else {
                            value += Double.valueOf(str);
                        }
                        count++;
                        continue;
                    }
                    return null;
                }
            }
        }
        if (count == 0) {
            return null;
        }
        keyValue.setKey(key);
        if (flag == 0) {
            keyValue.setValue(String.valueOf(longValue));
        } else {
            keyValue.setValue(String.valueOf(value));
        }
        return keyValue;
    }

    public static boolean isNumeric(String str) {
        Pattern pattern = Pattern.compile("^[-+]?\\d+(\\.\\d+)?$");
        Matcher isNum = pattern.matcher(str);
        if (!isNum.matches()) {
            return false;
        }
        return true;
    }
}
