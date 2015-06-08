package zcf.group.MR4_ITEM;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;


import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record key;
    private Record value;


    public void setup(TaskContext context) throws IOException {
        key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
        //one.setBigint(0, 1L);
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        //String w = record.getString(0);  // getString() 会把读入的数据转为字串
        //word.setString(0, w);
        //context.write(word, one); //输出键值对
    	//System.out.println("***************" + temp1);
    	
    	key.set("item_id", record.getString(1));
    	value.set("click_rate", record.getDouble(3));
    	value.set("colle_rate", record.getDouble(4));
    	value.set("cart_rate", record.getDouble(5));
    	value.set("sliceNum", record.getBigint(6));
    	value.set("buyNum", record.getBigint(7));
    	value.set("trainDays", record.getBigint(8));
    	value.set("is_buy", record.getBigint(9));
    	value.set("minBuyDay", record.getBigint(10));
    	value.set("maxBuyDay", record.getBigint(11));
    	value.set("buyDays", record.getBigint(12));
    	context.write(key, value);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}