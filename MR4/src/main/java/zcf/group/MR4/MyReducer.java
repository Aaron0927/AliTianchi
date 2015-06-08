package zcf.group.MR4;

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

        
        // 点击，加购，购买转换率
        double userClickRate = 0;
        double userColleRate = 0;
        double userCartRate = 0;
        
        // 用户购买力
        int userBuyAbility = 0;
         
        // 用户购买周期
        double userBuyPeriod = 100;
        boolean flag = false;
        
        // 用户涉猎广度
        int userClickItem = 0;
        int userBuyItem = 0;
        
        long buy = 0;
        while (values.hasNext()) {
            Record val = values.next();
            sum_click_rate += val.getDouble("click_rate");
            sum_colle_rate += val.getDouble("colle_rate");
            sum_cart_rate += val.getDouble("cart_rate");
            sum_slice += val.getBigint("sliceNum");
            buy = val.getBigint("buyNum");
            userBuyAbility += buy;
            if(buy != 0 ) {
            	// 计算狩猎广度
            	userBuyItem ++;
            }
            userClickItem ++;
            
            if (flag == false) {
            	// 这个只需要执行一次，保证执行效率
            	flag = true;
            	long s = val.getBigint("trainDays");
            	long t = val.getBigint("buyDays");
            	if (t != 0) {
            		userBuyPeriod =  1.0 * s / t;
            	}
            }
        }
        if (sum_slice != 0) {
            userClickRate = sum_click_rate / sum_slice;
            userColleRate = sum_colle_rate / sum_slice;
            userCartRate = sum_cart_rate / sum_slice;
        }

        
        result.set(0, key.getString("user_id"));
        result.set(1, userClickRate);
        result.set(2, userColleRate);
        result.set(3, userCartRate);
        result.set(4, userBuyAbility);
        result.set(5, userBuyPeriod);
        result.set(6, userBuyItem);
        result.set(7, userClickItem);
        context.write(result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
