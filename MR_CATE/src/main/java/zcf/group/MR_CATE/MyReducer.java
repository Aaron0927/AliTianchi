package zcf.group.MR_CATE;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }
    
    /**
	 * 求相对时间
	 * @param Date1
	 * @return 返回与基准时间(预测日)的相对天数
	 * 静态方法：静态方法不需要创建一个此类的对象即可使用
	 */
    public static long relativeTime(String Date1) {
    	DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	 	try {
	 		Date dt1 = df.parse(Date1);
	 		Date dt2 = df.parse("2014-12-19");

	 		long diff = dt2.getTime() - dt1.getTime();
	 		long days = diff / (1000 * 60 * 60 * 24);
	 		// 如果是预测日，不考虑;只处理观测日之前的数据
//	 		if (days < 0) {
//	 			days = 100;
//	 		}
	 		return days;
	  	} catch (Exception ex) {
	  		ex.printStackTrace();
	  	}
	  	return 100L;
    }
    
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        // 历史总行为上的统计
    	long big_category_action_all = 0L;
        long big_category_click_all = 0L;
        long big_category_collection_all = 0L;
        long big_category_cart_all = 0L;
        long big_category_buy_all = 0L;
        long big_category_buy_day_all = 0L;
        long big_category_buy_user_all = 0L; // 商品购买用户数目（distinct）
        double big_category_transfer_all = 0;
        double big_category_cart_coll_transfer_all = 0;
        double big_category_click_transfer_all = 0;
        double big_category_per_user_all = 0; // 人均銷量
        double big_category_per_day_all = 0; // 日均銷量
        
        // 近期行为统计
        long big_category_action_1 = 0L;
        long big_category_action_3 = 0L;
        long big_category_action_5 = 0L;
        
        long big_category_click_1 = 0L;
        long big_category_click_3 = 0L;
        long big_category_click_5 = 0L;
        
        long big_category_collection_1 = 0L;
        long big_category_collection_3 = 0L;
        long big_category_collection_5 = 0L;
        
        long big_category_cart_1 = 0L;
        long big_category_cart_3 = 0L;
        long big_category_cart_5 = 0L;
        
        long big_category_buy_1 = 0L;
        long big_category_buy_3 = 0L;
        long big_category_buy_5 = 0L;
        
        long big_category_buy_day_1 = 0L;
        long big_category_buy_day_3 = 0L;
        long big_category_buy_day_5 = 0L;
        
        long big_category_buy_user_1 = 0L;
        long big_category_buy_user_3 = 0L;
        long big_category_buy_user_5 = 0L;
        
        double big_category_transfer_1 = 0;
        double big_category_transfer_3 = 0;
        double big_category_transfer_5 = 0;
        
        double big_category_cart_coll_transfer_1 = 0;
        double big_category_cart_coll_transfer_3 = 0;
        double big_category_cart_coll_transfer_5 = 0;
        
        double big_category_click_transfer_1 = 0;
        double big_category_click_transfer_3 = 0;
        double big_category_click_transfer_5 = 0;
        
        
        double big_category_per_user_1 = 0; // 人均銷量
        double big_category_per_user_3 = 0; // 人均銷量
        double big_category_per_user_5 = 0; // 人均銷量
        
        double big_category_per_day_1 = 0; // 日均銷量
        double big_category_per_day_3 = 0; // 日均銷量
        double big_category_per_day_5 = 0; // 日均銷量
        
        double big_category_buy_increase = 0; //销量增长率
        
        long big_category_recent_action = 32;
        long big_category_recent_buy = 32;
        long big_category_recent_collection = 32;
        long big_category_recent_cart = 32;
        
    	Map<String, Long> userCounter = new TreeMap<String, Long>(); // 统计用户数目
    	Map<String, Long> dayCounter = new TreeMap<String, Long>(); // 统计商品总用出现天数
    	
    	Map<String, Long> userCounter1 = new TreeMap<String, Long>(); // 统计用户数目
    	Map<String, Long> userCounter3 = new TreeMap<String, Long>(); // 统计用户数目
    	Map<String, Long> userCounter5 = new TreeMap<String, Long>(); // 统计用户数目
    	Map<String, Long> dayCounter1 = new TreeMap<String, Long>(); // 统计商品总用出现天数
    	Map<String, Long> dayCounter3 = new TreeMap<String, Long>(); // 统计商品总用出现天数
    	Map<String, Long> dayCounter5 = new TreeMap<String, Long>(); // 统计商品总用出现天数
    	
    	long day = 0;
        boolean isOnlyBefore = false; // 只输出观测值前面的数据
        long temp_day2 = 0; // 记录第二天的销量
        while (values.hasNext()) {
            Record val = values.next();
            
            String time = val.getString("time").substring(0, 10);
            day = relativeTime(time);
    		if (day > 0 && day < 32) {
    			
    			isOnlyBefore = true;
    			int behavior_type = val.getBigint("behavior_type").intValue();
    			
				if (big_category_recent_action == 32) {
					big_category_recent_action = day;
				} else if (big_category_recent_action > day) {
					big_category_recent_action = day;
				}
				
				if (behavior_type == 4) {
					if (big_category_recent_buy == 32) {
						big_category_recent_buy = day;
					} else if (big_category_recent_buy > day) {
						big_category_recent_buy = day;
					}
					if (day == 2) {
						// 计算第二天的销量
						temp_day2 ++;
					}
				}
				
				if (behavior_type == 3) {
					if (big_category_recent_cart == 32) {
						big_category_recent_cart = day;
					} else if (big_category_recent_cart > day) {
						big_category_recent_cart = day;
					}
				}

				if (behavior_type == 2) {
					if (big_category_recent_collection == 32) {
						big_category_recent_collection = day;
					} else if (big_category_recent_collection > day) {
						big_category_recent_collection = day;
					}
				}
    			
    			
    			
    			
	            if (!dayCounter.containsKey(time)) {
	    			dayCounter.put(time, 1L);
	    		}
	            
	            big_category_action_all ++;
	            String user_id = val.getString("user_id");
	            
	            if (!userCounter.containsKey(user_id)) {
	    			userCounter.put(user_id, 1L);
	    		}
	            
	            if (day == 1) {
	            	// 前1天的行为统计
	            	big_category_action_1 ++;
		            if (behavior_type == 1) {
		            	big_category_click_1 ++;
		            } else if (behavior_type == 2) {
		            	big_category_collection_1 ++;
		            } else if (behavior_type == 3) {
		            	big_category_cart_1 ++;
		            }  else if (behavior_type == 4) {
		            	big_category_buy_1 ++;
		            } 
		            if (!dayCounter1.containsKey(time)) {
		    			dayCounter1.put(time, 1L);
		    		}
		            		            
		            if (!userCounter1.containsKey(user_id)) {
		    			userCounter1.put(user_id, 1L);
		    		}
	            }
	            if (day <= 3) {
	            	// 前3天行为统计
	            	big_category_action_3 ++;
		            if (behavior_type == 1) {
		            	big_category_click_3 ++;
		            } else if (behavior_type == 2) {
		            	big_category_collection_3 ++;
		            } else if (behavior_type == 3) {
		            	big_category_cart_3 ++;
		            }  else if (behavior_type == 4) {
		            	big_category_buy_3 ++;
		            } 
		            if (!dayCounter3.containsKey(time)) {
		    			dayCounter3.put(time, 1L);
		    		}
		            		            
		            if (!userCounter3.containsKey(user_id)) {
		    			userCounter3.put(user_id, 1L);
		    		}
	            }
	            
	            if (day <= 5) {
	            	// 前5天行为统计
	            	big_category_action_5 ++;
		            if (behavior_type == 1) {
		            	big_category_click_5 ++;
		            } else if (behavior_type == 2) {
		            	big_category_collection_5 ++;
		            } else if (behavior_type == 3) {
		            	big_category_cart_5 ++;
		            }  else if (behavior_type == 4) {
		            	big_category_buy_5 ++;
		            } 
		            if (!dayCounter5.containsKey(time)) {
		    			dayCounter5.put(time, 1L);
		    		}
		            		            
		            if (!userCounter5.containsKey(user_id)) {
		    			userCounter5.put(user_id, 1L);
		    		}
	            }
	            
	            
	            if (behavior_type == 1) {
	            	big_category_click_all ++;
	            } else if (behavior_type == 2) {
	            	big_category_collection_all ++;
	            } else if (behavior_type == 3) {
	            	big_category_cart_all ++;
	            }  else if (behavior_type == 4) {
	            	big_category_buy_all ++;
	            }  
    		}
            
        }
        big_category_buy_user_all = userCounter.size();
        big_category_buy_day_all = dayCounter.size();
        
        if (big_category_action_all > 0) {
        	big_category_transfer_all = 1.0 * big_category_buy_all / big_category_action_all;
        }
        if ((big_category_collection_all + big_category_cart_all) > 0) {
        	big_category_cart_coll_transfer_all = 1.0 * big_category_buy_all / (big_category_collection_all + big_category_cart_all);
        }
        if (big_category_click_all > 0) {
        	big_category_click_transfer_all = 1.0 * big_category_buy_all / big_category_click_all;
        }
        // 商品人均销量
        if (big_category_buy_user_all > 0) {
        	big_category_per_user_all = 1.0 * big_category_buy_all / big_category_buy_user_all;
        }
        // 品日均销量
        if (big_category_buy_day_all > 0) {
        	big_category_per_day_all = 1.0 * big_category_buy_all / big_category_buy_day_all;
        }
        
        big_category_buy_user_1 = userCounter1.size();
        big_category_buy_user_3 = userCounter3.size();
        big_category_buy_user_5 = userCounter5.size();
        
        big_category_buy_day_1 = dayCounter1.size();
        big_category_buy_day_3 = dayCounter3.size();
        big_category_buy_day_5 = dayCounter5.size();
        
        // 前1天行为
        if (big_category_action_1 > 0) {
        	big_category_transfer_1 = 1.0 * big_category_buy_1 / big_category_action_1;
        }
        if ((big_category_collection_1 + big_category_cart_1) > 0) {
        	big_category_cart_coll_transfer_1 = 1.0 * big_category_buy_1 / (big_category_collection_1 + big_category_cart_1);
        }
        if (big_category_click_1 > 0) {
        	big_category_click_transfer_1 = 1.0 * big_category_buy_1 / big_category_click_1;
        }
        // 商品人均销量
        if (big_category_buy_user_1 > 0) {
        	big_category_per_user_1 = 1.0 * big_category_buy_1 / big_category_buy_user_1;
        }
        // 品日均销量
        if (big_category_buy_day_1 > 0) {
        	big_category_per_day_1 = 1.0 * big_category_buy_1 / big_category_buy_day_1;
        }
        
        // 前3天行为
        if (big_category_action_3 > 0) {
        	big_category_transfer_3 = 1.0 * big_category_buy_3 / big_category_action_3;
        }
        if ((big_category_collection_3 + big_category_cart_3) > 0) {
        	big_category_cart_coll_transfer_3 = 1.0 * big_category_buy_3 / (big_category_collection_3 + big_category_cart_3);
        }
        if (big_category_click_3 > 0) {
        	big_category_click_transfer_3 = 1.0 * big_category_buy_3 / big_category_click_3;
        }
        // 商品人均销量
        if (big_category_buy_user_3 > 0) {
        	big_category_per_user_3 = 1.0 * big_category_buy_3 / big_category_buy_user_3;
        }
        // 品日均销量
        if (big_category_buy_day_3 > 0) {
        	big_category_per_day_3 = 1.0 * big_category_buy_3 / big_category_buy_day_3;
        }
        
        // 前5天行为
        if (big_category_action_5 > 0) {
        	big_category_transfer_5 = 1.0 * big_category_buy_5 / big_category_action_5;
        }
        if ((big_category_collection_5 + big_category_cart_5) > 0) {
        	big_category_cart_coll_transfer_5 = 1.0 * big_category_buy_5 / (big_category_collection_5 + big_category_cart_5);
        } 
        if (big_category_click_5 > 0) {
        	big_category_click_transfer_5 = 1.0 * big_category_buy_5 / big_category_click_5;
        }
        // 商品人均销量
        if (big_category_buy_user_5 > 0) {
        	big_category_per_user_5 = 1.0 * big_category_buy_5 / big_category_buy_user_5;
        }
        // 品日均销量
        if (big_category_buy_day_5 > 0) {
        	big_category_per_day_5 = 1.0 * big_category_buy_5 / big_category_buy_day_5;
        }
        long temp_day3 = (big_category_buy_3 - big_category_buy_1 - temp_day2); // 第3天销量
        double rate1 = 0;
        double rate2 = 0;
        if (temp_day2 > 0) {
        	rate1 = (big_category_buy_1 - temp_day2) * 1.0 / temp_day2;
        }
        if (temp_day3 > 0) {
    		rate2 = (temp_day2 - temp_day3) / temp_day3;
    	}
        big_category_buy_increase = (rate1 + rate2) / 2.0;
        
        
        if (isOnlyBefore == true) {
        	result.set(0, key.get(0));
        	result.set(1, big_category_action_all);
        	result.set(2, big_category_click_all);
        	result.set(3, big_category_collection_all);
        	result.set(4, big_category_cart_all);
        	result.set(5, big_category_buy_all);
        	result.set(6, big_category_buy_day_all);
        	result.set(7, big_category_buy_user_all);
        	result.set(8, big_category_transfer_all);
        	result.set(9, big_category_cart_coll_transfer_all);
        	result.set(10, big_category_click_transfer_all);
        	result.set(11, big_category_per_user_all);
        	result.set(12, big_category_per_day_all);
        	result.set(13, big_category_action_1);
        	result.set(14, big_category_action_3);
        	result.set(15, big_category_action_5);
        	result.set(16, big_category_click_1);
        	result.set(17, big_category_click_3);
        	result.set(18, big_category_click_5);
        	result.set(19, big_category_collection_1);
        	result.set(20, big_category_collection_3);
        	result.set(21, big_category_collection_5);
        	result.set(22, big_category_cart_1);
        	result.set(23, big_category_cart_3);
        	result.set(24, big_category_cart_5);
        	result.set(25, big_category_buy_1);
        	result.set(26, big_category_buy_3);
        	result.set(27, big_category_buy_5);
        	result.set(28, big_category_buy_day_1);
        	result.set(29, big_category_buy_day_3);
        	result.set(30, big_category_buy_day_5);
        	result.set(31, big_category_buy_user_1);
        	result.set(32, big_category_buy_user_3);
        	result.set(33, big_category_buy_user_5);
        	result.set(34, big_category_transfer_1);
        	result.set(35, big_category_transfer_3);
        	result.set(36, big_category_transfer_5);
        	result.set(37, big_category_cart_coll_transfer_1);
        	result.set(38, big_category_cart_coll_transfer_3);
        	result.set(39, big_category_cart_coll_transfer_5);
        	result.set(40, big_category_click_transfer_1);
        	result.set(41, big_category_click_transfer_3);
        	result.set(42, big_category_click_transfer_5);
        	result.set(43, big_category_per_user_1);
        	result.set(44, big_category_per_user_3);
        	result.set(45, big_category_per_user_5);
        	result.set(46, big_category_per_day_1);
        	result.set(47, big_category_per_day_3);
        	result.set(48, big_category_per_day_5);
        	result.set(49, big_category_buy_increase);
        	result.set(50, big_category_recent_action);
        	result.set(51, big_category_recent_buy);
        	result.set(52, big_category_recent_collection);
        	result.set(53, big_category_recent_cart);
        	context.write(result);
        }
        
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
