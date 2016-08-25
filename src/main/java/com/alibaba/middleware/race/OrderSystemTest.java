package com.alibaba.middleware.race;

import com.alibaba.middleware.race.model.KeyValue;
import com.alibaba.middleware.race.model.Result;
import com.alibaba.middleware.race.util.FileUtil;
import com.alibaba.middleware.race.util.ProduceData;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class OrderSystemTest {

    OrderSystem orderSystem = new OrderSystemImpl();

    @Test
    public void testQueryOrder() {
        // 测试queryOrder接口，按订单号查找某条记录
        List<String> keys = new ArrayList<String>();
        keys.add("buyerid");
        keys.add("amount");
        keys.add("buyername");
        keys.add("good_name");
        keys.add("orderid");
        System.out.println("\n测试queryOrder接口，按订单号查找某条记录: ");
        Result result = (Result) orderSystem.queryOrder(2982139, null);
        System.out.println(result.get("buyerid").getValue());
        System.out.println(result.get("amount").getValue());
        System.out.println(result.get("buyername").getValue());
        System.out.println(result.get("good_name").getValue());
        System.out.println(result.get("orderid").getValue());
    }

    @Test
    public void testQueryOrdersByBuyer() {
        // 测试queryOrderByBuyer接口，查找某个买家在某个时间段的所有记录
        System.out.println("\n测试queryOrderByBuyer接口，查找某个买家在某个时间段的所有记录: ");
        Iterator<Result> resultIterator = orderSystem.queryOrdersByBuyer(1463076523, 1465018171, "ap_236ed7ca-dcb9-4562-8b35-072834c45d18");
        while (resultIterator.hasNext()) {
            System.out.println("===============");
            Result result2 = resultIterator.next();
            System.out.println(result2.get("createtime").getValue());
        }
    }

    @Test
    public void testQueryOrdersBySaler() {
        // 测试queryOrderBySaler接口，查找某个卖家的某个商品的所有记录信息
        List<String> keys = new ArrayList<String>();
        keys.add("buyerid");
        keys.add("amount");
        System.out.println("\n测试queryOrderBySaler接口，查找某个卖家的某个商品的所有记录信息: ");
        Iterator<Result> resultIterator2 = orderSystem.queryOrdersBySaler("", "goodal_a289ad59-2660-42af-8618-018fd161c391", null);
        while (resultIterator2.hasNext()) {
            Result result3 = resultIterator2.next();
            System.out.println(result3.get("goodid").getValue());
        }
    }

    @Test
    public void testSumOrdersByGood() {
        // 测试sumOrdersByGood接口，查找某个商品的某个属性的聚合值
        System.out.println("\n测试sumOrdersByGood接口，查找某个商品的某个属性的聚合值: ");
        KeyValue keyValue = (KeyValue) orderSystem.sumOrdersByGood("goodal_a289ad59-2660-42af-8618-018fd161c391", "amount");
        System.out.println(keyValue.getKey() + ": " + keyValue.getValue());
    }

    @Test
    public void testProduceData() {
        // 测试sumOrdersByGood接口，查找某个商品的某个属性的聚合值
        System.out.println("\n测试produceData: ");
        ProduceData.produceOrderData(10000);

    }

    @Test
    public void testBuyeridIndex() {
        // 测试sumOrdersByGood接口，查找某个商品的某个属性的聚合值
//        System.out.println("\n测试buyerid生成一级二级索引: ");
//        BuyerIdIndexFile buyerIdIndexFile = new BuyerIdIndexFile(null, null, 0);
//        buyerIdIndexFile.generateBuyerIdIndex();
//        String str = "ap_236ed7ca-dcb9-4562-8b35-072834c45d18";
//        int hashIndex = Math.abs(str.hashCode())
//                % Config.ORDER_ONE_INDEX_FILE_NUMBER;
//        BuyerIdQuery.findByBuyerId("ap_236ed7ca-dcb9-4562-8b35-072834c45d18",
//                1463076523, 1465018171, hashIndex);
    }

    static {

        OrderSystem orderSystem = new OrderSystemImpl();
        List<String> orderFileList = new ArrayList<String>();
        orderFileList.add("orderrecords.txt");
        // orderFileList.add("order.0.0");
        // orderFileList.add("order.0.3");
        // orderFileList.add("order.1.1");
        // orderFileList.add("order.2.2");
        // for (int i = 0; i < 10; i++) {
        // orderFileList.add("order.3." + i);
        // }

        List<String> buyerFileList = new ArrayList<String>();
        buyerFileList.add("buyerrecords.txt");
        // buyerFileList.add("buyer.0.0");
        // buyerFileList.add("buyer.1.1");

        List<String> goodFileList = new ArrayList<String>();
        goodFileList.add("goodrecords.txt");

        List<String> storeFolderList = new ArrayList<String>();
        FileUtil.createDir("./s1");
        FileUtil.createDir("./s2");
        FileUtil.createDir("./s3");
        storeFolderList.add("./s1/");
        storeFolderList.add("./s2/");
        storeFolderList.add("./s3/");

        try {
            orderSystem.construct(orderFileList, buyerFileList, goodFileList,
                    storeFolderList);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
