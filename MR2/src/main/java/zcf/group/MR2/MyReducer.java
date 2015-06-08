package zcf.group.MR2;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;

    public static long relativeTime(String Date1) {
    	DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	 	try {
	 		Date dt1 = df.parse(Date1);
	 		Date dt2 = df.parse("2014-12-19");

	 		long diff = dt2.getTime() - dt1.getTime();
	 		long days = diff / (1000 * 60 * 60 * 24);
	 		// 如果是预测日，不考虑;只处理观测日之前的数据
	 		if (days < 0) {
	 			days = 100;
	 		}
	 		return days;
	  	} catch (Exception ex) {
	  		ex.printStackTrace();
	  	}
	  	return 100L;
    }
    
    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
    	int user_item_is_click1 = 0; // 前一天是否点击过
    	int user_item_is_click2 = 0; // 前两天是否点击过
    	int user_item_is_click3 = 0;
    	int user_item_is_click4 = 0;
    	int user_item_is_click5 = 0;
    	int user_item_is_click6 = 0; // 前3小时是否点击过[21,00)
    	int user_item_is_click7 = 0; // 前6小时是否点击过[18,21)
    	int user_item_is_click8 = 0; // 前9小时是够点击过[15,18)
    	int user_item_is_click9 = 0; // 前12小时是够点击过[12,15)
    	int user_item_is_click10 = 0; // 前15小时是够点击过[9,12)
    	int user_item_is_click11 = 0; // 前18小时是够点击过[6,9)
    	int user_item_is_click12 = 0; // 前21小时是够点击过[3,6)
    	int user_item_is_click13 = 0; // 前24小时是够点击过[0,3)

    	int user_item_is_cart1 = 0; // 前一天是否点加购
    	int user_item_is_cart2 = 0; // 前两天是否点加购
    	int user_item_is_cart3 = 0;
    	int user_item_is_cart4 = 0;
    	int user_item_is_cart5 = 0;
    	int user_item_is_cart6 = 0; // 前3小时是否点加购[21,00)
    	int user_item_is_cart7 = 0; // 前6小时是否点加购[18,21)
    	int user_item_is_cart8 = 0; // 前9小时是够点加购[15,18)
    	int user_item_is_cart9 = 0; // 前12小时是够点加购[12,15)
    	int user_item_is_cart10 = 0; // 前15小时是够点加购[9,12)
    	int user_item_is_cart11 = 0; // 前18小时是够点加购[6,9)
    	int user_item_is_cart12 = 0; // 前21小时是够点加购[3,6)
    	int user_item_is_cart13 = 0; // 前24小时是够点加购[0,3)
    	
    	int user_item_is_colle1 = 0; // 前一天是否点收藏
    	int user_item_is_colle2 = 0; // 前两天是否点收藏
    	int user_item_is_colle3 = 0;
    	int user_item_is_colle4 = 0;
    	int user_item_is_colle5 = 0;
    	int user_item_is_colle6 = 0; // 前3小时是否点收藏[21,00)
    	int user_item_is_colle7 = 0; // 前6小时是否点收藏[18,21)
    	int user_item_is_colle8 = 0; // 前9小时是够点收藏[15,18)
    	int user_item_is_colle9 = 0; // 前12小时是够点收藏[12,15)
    	int user_item_is_colle10 = 0; // 前15小时是够点收藏[9,12)
    	int user_item_is_colle11 = 0; // 前18小时是够点收藏[6,9)
    	int user_item_is_colle12 = 0; // 前21小时是够点收藏[3,6)
    	int user_item_is_colle13 = 0; // 前24小时是够点收藏[0,3)
    	
    	int itemID = 0;
    	long day;
    	int hour;
        while (values.hasNext()) {
            Record val = values.next();
            String time = val.getString("time");
    		itemID = Integer.parseInt(val.getString("item_category"));
    		// 求相对天数
    		day = relativeTime(time.substring(0, 10));
    		
    		// 获取小时数
    		hour = Integer.parseInt(time.substring(11, 13));
    		
    		int behavior_type = val.getBigint("behavior_type").intValue();
    		//String days = String.format("%02d", day);
    		if (day == 1) {
    			// 前1天行为
    			if (behavior_type == 1) {
    				user_item_is_click1 = 1;
    				if (hour < 24 && hour >= 21) {
    					user_item_is_click6 = 1;
    				} else if (hour < 21 && hour >= 18) {
    					user_item_is_click7 = 1;
    				} else if (hour < 18 && hour >= 15) {
    					user_item_is_click8 = 1;
    				} else if (hour < 15 && hour >= 12) {
    					user_item_is_click9 = 1;
    				} else if (hour < 12 && hour >= 9) {
    					user_item_is_click10 = 1;
    				} else if (hour < 9 && hour >= 6) {
    					user_item_is_click11 = 1;
    				} else if (hour < 6 && hour >= 3) {
    					user_item_is_click12 = 1;
    				} else if (hour < 3 && hour >= 0) {
    					user_item_is_click13 = 1;
    				}
    			} else if (behavior_type == 2) {
    				user_item_is_colle1 = 1;
    				if (hour < 24 && hour >= 21) {
    					user_item_is_colle6 = 1;
    				} else if (hour < 21 && hour >= 18) {
    					user_item_is_colle7 = 1;
    				} else if (hour < 18 && hour >= 15) {
    					user_item_is_colle8 = 1;
    				} else if (hour < 15 && hour >= 12) {
    					user_item_is_colle9 = 1;
    				} else if (hour < 12 && hour >= 9) {
    					user_item_is_colle10 = 1;
    				} else if (hour < 9 && hour >= 6) {
    					user_item_is_colle11 = 1;
    				} else if (hour < 6 && hour >= 3) {
    					user_item_is_colle12 = 1;
    				} else if (hour < 3 && hour >= 0) {
    					user_item_is_colle13 = 1;
    				}
    			} else if (behavior_type == 3) {
    				user_item_is_cart1 = 1;
    				if (hour < 24 && hour >= 21) {
    					user_item_is_cart6 = 1;
    				} else if (hour < 21 && hour >= 18) {
    					user_item_is_cart7 = 1;
    				} else if (hour < 18 && hour >= 15) {
    					user_item_is_cart8 = 1;
    				} else if (hour < 15 && hour >= 12) {
    					user_item_is_cart9 = 1;
    				} else if (hour < 12 && hour >= 9) {
    					user_item_is_cart10 = 1;
    				} else if (hour < 9 && hour >= 6) {
    					user_item_is_cart11 = 1;
    				} else if (hour < 6 && hour >= 3) {
    					user_item_is_cart12 = 1;
    				} else if (hour < 3 && hour >= 0) {
    					user_item_is_cart13 = 1;
    				}
    			}
    		} else if (day == 2) {
    			// 前2天行为
    			if (behavior_type == 1) {
    				user_item_is_click2 = 1;
    			} else if (behavior_type == 2) {
    				user_item_is_colle2 = 1;
    			} else if (behavior_type == 3) {
    				user_item_is_cart2 = 1;
    			}
    		} else if (day == 3) {
    			// 前3天行为
    			if (behavior_type == 1) {
    				user_item_is_click3 = 1;
    			} else if (behavior_type == 2) {
    				user_item_is_colle3 = 1;
    			} else if (behavior_type == 3) {
    				user_item_is_cart3 = 1;
    			}
    		} else if (day == 4) {
    			// 前4天行为
    			if (behavior_type == 1) {
    				user_item_is_click4 = 1;
    			} else if (behavior_type == 2) {
    				user_item_is_colle4 = 1;
    			} else if (behavior_type == 3) {
    				user_item_is_cart4 = 1;
    			}
    		} else if (day == 5) {
    			// 前5天行为
    			if (behavior_type == 1) {
    				user_item_is_click5 = 1;
    			} else if (behavior_type == 2) {
    				user_item_is_colle5 = 1;
    			} else if (behavior_type == 3) {
    				user_item_is_cart5 = 1;
    			}
    		} 
    		

        }


        
        result.set(0, key.getString("user_id"));

        context.write(result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
