package zcf.group.Geo02;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
// 下面是用于时间转换
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
// 自然对数E
import java.lang.Math;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
    	// 用交互过的用户信息进行恢复
    	
    	
    	// 存储用户历史信息中地理位置每bit的geohash
    	Map<Character, Long> g1 = new TreeMap<Character, Long>();
    	Map<Character, Long> g2 = new TreeMap<Character, Long>();
    	Map<Character, Long> g3 = new TreeMap<Character, Long>();
    	Map<Character, Long> g4 = new TreeMap<Character, Long>();
    	Map<Character, Long> g5 = new TreeMap<Character, Long>();
    	Map<Character, Long> g6 = new TreeMap<Character, Long>();
    	Map<Character, Long> g7 = new TreeMap<Character, Long>();
    	
    	boolean is_miss = true;
    	boolean is_no_history_geohash = true; // 标记是否有用户历史行为
        while (values.hasNext()) {
            Record val = values.next();
            if (val.get("item_geohash") != null) {
            	// 只要有一次不为空就说明不缺失
            	is_miss = false;
            }
            if (val.get("user_geohash") != null) {
            	is_no_history_geohash = false;
            	char user_geohash []= val.getString("user_geohash").toCharArray();
                // 对user_geohash进行存储
            	char geoKey = user_geohash[0];
                if (g1.containsKey(geoKey)) {
                	g1.put(geoKey, g1.get(geoKey) + 1L);
                } else {
                	g1.put(geoKey, 1L);
                }
                
            	geoKey = user_geohash[1];
                if (g2.containsKey(geoKey)) {
                	g2.put(geoKey, g2.get(geoKey) + 1L);
                } else {
                	g2.put(geoKey, 1L);
                }
                
                geoKey = user_geohash[2];
                if (g3.containsKey(geoKey)) {
                	g3.put(geoKey, g3.get(geoKey) + 1L);
                } else {
                	g3.put(geoKey, 1L);
                }
                
                geoKey = user_geohash[3];
                if (g4.containsKey(geoKey)) {
                	g4.put(geoKey, g4.get(geoKey) + 1L);
                } else {
                	g4.put(geoKey, 1L);
                }
                
                geoKey = user_geohash[4];
                if (g5.containsKey(geoKey)) {
                	g5.put(geoKey, g5.get(geoKey) + 1L);
                } else {
                	g5.put(geoKey, 1L);
                }
                
                geoKey = user_geohash[5];
                if (g6.containsKey(geoKey)) {
                	g6.put(geoKey, g6.get(geoKey) + 1L);
                } else {
                	g6.put(geoKey, 1L);
                }
                
                geoKey = user_geohash[6];
                if (g7.containsKey(geoKey)) {
                	g7.put(geoKey, g7.get(geoKey) + 1L);
                } else {
                	g7.put(geoKey, 1L);
                }    
            }
            
                      
            
        }
        if (is_miss == true && is_no_history_geohash == false) { // 信息丢失&&有历史信息 --> 用历史信息恢复
        	char user_geohash [] = {' ', ' ', ' ', ' ', ' ', ' ', ' '}; //最终的最大子串
        	char bigStringSet; // 记录每位的最大（数目最多）char
        	long num; // 标记最大次数
        	
        	// 查找最大串
        	bigStringSet = ' ';
        	num = 0L;
        	for(Map.Entry<Character, Long> entry : g1.entrySet()) {
        		if (num < entry.getValue()) {
        			num = entry.getValue();
        			bigStringSet = entry.getKey();
        		}
        	}
        	user_geohash[0] = bigStringSet;
        	
        	bigStringSet = ' ';
        	num = 0L;
        	for(Map.Entry<Character, Long> entry : g2.entrySet()) {
        		if (num < entry.getValue()) {
        			num = entry.getValue();
        			bigStringSet = entry.getKey();
        		}
        	}
        	user_geohash[1] = bigStringSet;
        	
        	bigStringSet = ' ';
        	num = 0L;
        	for(Map.Entry<Character, Long> entry : g3.entrySet()) {
        		if (num < entry.getValue()) {
        			num = entry.getValue();
        			bigStringSet = entry.getKey();
        		}
        	}
        	user_geohash[2] = bigStringSet;
        	
        	bigStringSet = ' ';
        	num = 0L;
        	for(Map.Entry<Character, Long> entry : g4.entrySet()) {
        		if (num < entry.getValue()) {
        			num = entry.getValue();
        			bigStringSet = entry.getKey();
        		}
        	}
        	user_geohash[3] = bigStringSet;
        	
        	bigStringSet = ' ';
        	num = 0L;
        	for(Map.Entry<Character, Long> entry : g5.entrySet()) {
        		if (num < entry.getValue()) {
        			num = entry.getValue();
        			bigStringSet = entry.getKey();
        		}
        	}
        	user_geohash[4] = bigStringSet;
        	
        	bigStringSet = ' ';
        	num = 0L;
        	for(Map.Entry<Character, Long> entry : g6.entrySet()) {
        		if (num < entry.getValue()) {
        			num = entry.getValue();
        			bigStringSet = entry.getKey();
        		}
        	}
        	user_geohash[5] = bigStringSet;
        	
        	bigStringSet = ' ';
        	num = 0L;
        	for(Map.Entry<Character, Long> entry : g7.entrySet()) {
        		if (num < entry.getValue()) {
        			num = entry.getValue();
        			bigStringSet = entry.getKey();
        		}
        	}
        	user_geohash[6] = bigStringSet;
	        result.set(0, key.get(0));
	        result.set(1, String.valueOf(user_geohash));
	        context.write(result);
        } 
        
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
