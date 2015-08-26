package zcf.group.MR2;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Mapper.TaskContext;

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
    	key.set("user_id", record.getString(0));
    	key.set("item_id", record.getString(1));
    	value.set("behavior_type", record.getBigint(2));
    	value.set("user_geohash", record.getString(3));
    	value.set("item_category", record.getString(4));
    	value.set("time", record.getString(5));
    	
    	context.write(key, value);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}