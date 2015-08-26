package zcf.group.MR4_ITEM;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        double sum_click_rate = 0;
        double sum_colle_rate = 0;
        double sum_cart_rate = 0;
        int sum_slice = 0;
        long slice = 0;
        
        // 点击，加购，购买转换率
        double itemClickRate = 0;
        double itemColleRate = 0;
        double itemCartRate = 0;
        
        // 营销周期
        long sumPeriod = 0;
        long userNum = 0;
        double itemPeriod = 100;
        
        // 商品热度
        long itemClickuser = 0;
        long itemBuyuser = 0;
        
        // 品牌的再购买率
        double itemP1 = 0; // 买过一次之后还会购买的概率
        double itemP2 = 0; // 买过两次之后还会购买的概率
        double itemP3 = 0; // 购买过三次之后还会购买的概率
        double itemP4 = 0; // 购买过四次之后还会购买的概率
        double itemP5 = 0; // 购买过5次之后还会购买的概率
        
        // 统计购买过一次，两次。。。的用户数目
        long buy001user = 0;
        long buy002user = 0;
        long buy003user = 0;
        long buy004user = 0;
        long buy005user = 0;
        long buy006user = 0; //大于等于6次
        while (values.hasNext()) {
            Record val = values.next();
            sum_click_rate += val.getDouble("click_rate");
            sum_colle_rate += val.getDouble("colle_rate");
            sum_cart_rate += val.getDouble("cart_rate");
            slice = val.getBigint("sliceNum");
            sum_slice += slice;
            if (slice >= 2) {
            	// 超过一次购买
            	sumPeriod += 1.0 * (val.getBigint("maxBuyDay") - val.getBigint("minBuyDay")) / slice;
            	userNum ++;
            }
            if (slice > 0) {
            	itemBuyuser ++;
                if (slice == 1) {
                	buy001user ++;
                } else if (slice == 2) {
                	buy002user ++;
                } else if (slice == 3) {
                	buy003user ++;
                } else if (slice == 4) {
                	buy004user ++;
                } else if (slice == 5) {
                	buy005user ++;
                } else {
                	buy006user ++;
                }
            }
            itemClickuser ++;
        }
        if (userNum > 0) {
        	itemPeriod = 1.0 * sumPeriod / userNum;
        }
        if ((buy001user + buy002user + buy003user + buy004user + buy005user + buy006user) > 0) {
        	itemP1 = 1.0 * (buy002user + buy003user + buy004user + buy005user + buy006user) / (buy001user + buy002user + buy003user + buy004user + buy005user + buy006user);
        }
        if ((buy002user + buy003user + buy004user + buy005user + buy006user) > 0) {
        	itemP2 = 1.0 * (buy003user + buy004user + buy005user + buy006user) / (buy002user + buy003user + buy004user + buy005user + buy006user);
        }
        if ((buy003user + buy004user + buy005user + buy006user) > 0) {
        	itemP3 = 1.0 * (buy004user + buy005user + buy006user) / (buy003user + buy004user + buy005user + buy006user);
        }
        if ((buy004user + buy005user + buy006user) > 0) {
        	itemP4 = 1.0 * (buy005user + buy006user) / (buy004user + buy005user + buy006user);
        }
        if (buy005user + buy006user > 0) {
        	itemP5 = 1.0 * buy005user / (buy005user + buy006user);
        }
        if (sum_slice != 0) {
        	itemClickRate = 1.0 * sum_click_rate / sum_slice;
        	itemColleRate = 1.0 * sum_colle_rate / sum_slice;
        	itemCartRate = 1.0 * sum_cart_rate / sum_slice;
        }
        
        result.set(0, key.getString("item_id"));
        result.set(1, itemClickRate);
        result.set(2, itemColleRate);
        result.set(3, itemCartRate);
        result.set(4, itemPeriod);
        result.set(5, itemClickuser);
        result.set(6, itemBuyuser);
        result.set(7, itemP1);
        result.set(8, itemP2);
        result.set(9, itemP3);
        result.set(10, itemP4);
        result.set(11, itemP5);
        
        context.write(result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
