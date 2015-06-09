package zcf.group.MR2;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.Reducer.TaskContext;

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
    // 静态方法是不能访问非静态变量的
    // 非静态方法可以访问静态变量
 	private static Map<Integer, Double[]> HourClickDec; // 时间衰减函数
 	private static Map<Integer, Double[]> HourCollecDec;
 	private static Map<Integer, Double> DayClickDec;
 	private static Map<Integer, Double> DayCollecDec;
 	private static Map<Integer, Double> Click2CollecRate; // 点击加购权重比

    // 线上运行共需修改三个地方
    // 1.预测日期
    // 2.输入表
    // 3.输出表
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
    /**
	 * 求基于时间衰减因子和行为特征的属性值
	 * @param day:距离预测日的天数
	 * @param counter:这一天各类行为数目
	 * @param w1,w2衰减因子
	 * @return 属性值
	 */
    public static double calculateClick(int day, Long [][] counter) {
    	double y = 0;    
    	     	
    	// 时间衰减系数，click_dec是点击的衰减，collec_dec是加购或收藏的衰减
    	// k是加购权值增益
 	
    	if (counter == null) {
    		// 为空时，赋值为0
    		// counter = zeroInit();
    	} else {
	    	for (int i = 0; i < 24; ++i) {
	    			y += domino(counter[i][0]) * HourClickDec.get(day)[i];
	    		}
	    }
    	// 评分公式
    	return y;
    }
    
    
    public static double calculateCart(int day, Long [][] counter) {
    	double y = 0;    
    	     	
    	// 时间衰减系数，click_dec是点击的衰减，collec_dec是加购或收藏的衰减
    	// k是加购权值增益
    	if (counter == null) {
    		// 为空时，赋值为0
    		// counter = zeroInit();
    	} else {
	    	for (int i = 0; i < 24; ++i) {
	    		// 查找是否有非零项，即是否有加购和收藏行为
	    		if ((counter[i][1] + counter[i][2]) > 0) {
		    		y += domino(counter[i][1] + counter[i][2]) * HourCollecDec.get(day)[i];

	    			}
	    		}
	    	}
    	// 评分公式
    	return y;
    }
    
    /**
	 * 计算多米诺效应，连续的点击会传递给下一个
	 * @param x:点击次数
	 * @return 经过多米诺效应的加权值，但不能无限制的加权，要减枝，在第十张骨牌倒塌就停止
	 * 		   这个时候，1次点击相当与原来的2.59次
	 */
    public static double domino(Long x) {
    	double sum = 1;
    	for (int i = 1; i < x; ++i) {
    		sum += Math.min(Math.pow(1.1, i), Math.pow(1.1, 10));
    	}
    	if (x == 0) {
    		sum = 0;
    	}
    	return sum;
    }
	 
 
    /**
	 * Map初始化
	 * @return 属性值
	 */
	public static Long [][] zeroInit() {
		Long [][] zero;
     	// 用于后面数组初始化
		zero = new Long[][]{new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, 
				new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, 
				new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, 
				new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, 
				new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, 
				new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, 
				new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, 
				new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}, new Long[]{0L, 0L, 0L, 0L}};
		return zero;
	}

	
    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
        HourClickDec = new TreeMap<Integer, Double[]>(); 
        HourCollecDec = new TreeMap<Integer, Double[]>();
     	DayClickDec = new TreeMap<Integer, Double>();
     	DayCollecDec = new TreeMap<Integer, Double>();
     	Click2CollecRate = new TreeMap<Integer, Double>();
     	
        // 前i天点击衰减系数
        HourClickDec.put(1, new Double[]{0.004509557,0.002447625,0.001279517,
        		0.000883525,0.000630401,0.000984215,0.00155036,0.002680866,
        		0.004085282,0.005190074,0.006512943,0.00716303,0.007470461,
        		0.007825236,0.008288737,0.009529815,0.01061165,0.01071373,
        		0.011800752,0.01569875,0.020293157,0.025184428,0.02833909,
        		0.029763913});
        
        HourClickDec.put(2, new Double[]{0.002333113,0.001181577,0.000720739,
        		0.000462124,0.00035863,0.000508655,0.000857392,0.00166922,
        		0.002301619,0.002715596,0.003332256,0.003580204,0.003631984,
        		0.003940394,0.004101333,0.004438084,0.004276773,0.004239252,
        		0.004572053,0.005915941,0.007207803,0.008837265,0.008909822,
        		0.007057732});
        
        HourClickDec.put(3, new Double[]{0.001730443,0.00085393,0.000525292,
        		0.000327356,0.000284279,0.000283774,0.000581112,0.001094618,
        		0.001575159,0.001958244,0.002280852,0.002575074,0.002616152,
        		0.002858453,0.002664368,0.002712883,0.002800652,0.002550315,
        		0.00270086,0.003529083,0.004133787,0.005043607,0.005112787,
        		0.003871421});
        
        HourClickDec.put(4, new Double[]{0.001346437,0.000724574,0.000453413,
        		0.000304903,0.000199379,0.000268192,0.00053706,0.000825651,
        		0.001062823,0.001527833,0.001928312,0.0019588,0.001990175,
        		0.002235219,0.001994778,0.002142606,0.002203029,0.001916138,
        		0.002165064,0.00268845,0.003263711,0.003821359,0.003765767,
        		0.002935182});
        
        HourClickDec.put(5, new Double[]{0.001070372,0.000603567,0.000350956,
        		0.000220084,0.000157059,0.00020305,0.000354176,0.000751591,
        		0.001005029,0.001171773,0.00148709,0.00146371,0.00149759,
        		0.001650105,0.001687688,0.001649282,0.001687607,0.001694163,
        		0.00164813,0.002145325,0.002805645,0.002880596,0.003017587,
        		0.002274484});
        
        HourClickDec.put(6, new Double[]{0.000884907,0.000502036,0.000250986,
        		0.000150449,0.000119609,0.00015504,0.00032126,0.000573539,
        		0.000806804,0.001013388,0.001159689,0.001077738,0.001235014,
        		0.001345578,0.001437368,0.001390235,0.001441814,0.001341711,
        		0.001380054,0.001691511,0.002085749,0.002441631,0.002307487,
        		0.001797802});     
        
        // 前i天加购或收藏衰减系数
        HourCollecDec.put(1, new Double[]{0.001992537,0.001209293,0.000566855,
        		0.000440427,0.000253458,0.000397775,0.00061242,0.000948937,
        		0.001392454,0.001741986,0.002244426,0.002469567,0.002528817,
        		0.002891177,0.002868147,0.003612346,0.004040582,0.004155931,
        		0.004565737,0.006324795,0.008599416,0.011362885,0.013461343,
        		0.015594015});
        
        HourCollecDec.put(2, new Double[]{0.000937822,0.000451395,0.000303506,
        		0.000201616,0.000157808,0.000190207,0.000257064,0.000532273,
        		0.000611215,0.000929189,0.001088605,0.00110516,0.001187026,
        		0.001305955,0.001390586,0.00153606,0.001457409,0.001569615,
        		0.001535752,0.002067876,0.002701686,0.003344081,0.003714502,
        		0.003100089});
        
        HourCollecDec.put(3, new Double[]{0.000665996,0.000340639,0.000267101,
        		0.000141089,8.80388E-05,9.87061E-05,0.000159666,0.000290023,
        		0.000450023,0.000572546,0.00069092,0.000849586,0.000819104,
        		0.000865177,0.000901065,0.000941626,0.000906297,0.000890075,
        		0.000847889,0.001134496,0.001372072,0.001877754,0.001987709,
        		0.001725793});
        
        HourCollecDec.put(4, new Double[]{0.000478006,0.000258003,0.000204735,
        		0.000112107,7.88315E-05,0.000117372,0.000151324,0.000218307,
        		0.00024572,0.000445831,0.000644568,0.000566782,0.000611773,
        		0.000714998,0.000608175,0.000668544,0.000703212,0.000629174,
        		0.000616488,0.000784747,0.001085385,0.001385193,0.001395014,
        		0.001139543});
        
        HourCollecDec.put(5, new Double[]{0.000381039,0.000193752,0.000130511,
        		0.000107111,4.77483E-05,7.44496E-05,0.000140062,0.00023298,
        		0.000295833,0.000363391,0.000423719,0.000459418,0.000438563,
        		0.000434954,0.00054562,0.000571617,0.000622178,0.000593556,
        		0.000474946,0.00068973,0.000921139,0.001028645,0.001048938,
        		0.000887521});
        
        HourCollecDec.put(6, new Double[]{0.000331965,0.00017624,4.48351E-05,
        		3.04793E-05,4.15799E-05,5.58011E-05,8.52071E-05,0.000197269,
        		0.000210246,0.000252295,0.000297589,0.000348467,0.000340479,
        		0.000401355,0.000432745,0.000436911,0.000489762,0.000416694,
        		0.000459078,0.000546368,0.000762368,0.000787317,0.000832785,
        		0.000639951});  
        
     	DayClickDec.put(1, 0.164099101);
     	DayClickDec.put(2, 0.067894517);
     	DayClickDec.put(3, 0.043171861);
     	DayClickDec.put(4, 0.033069282);
     	DayClickDec.put(5, 0.02643343);
     	DayClickDec.put(6, 0.020682993);
     	
     	DayCollecDec.put(1, 0.089767463);
     	DayCollecDec.put(2, 0.031089991);
     	DayCollecDec.put(3, 0.018388656);
     	DayCollecDec.put(4, 0.013330837);
     	DayCollecDec.put(5, 0.011006065);
     	DayCollecDec.put(6, 0.007982888);
     	
     	Click2CollecRate.put(1, 5.734642417);
     	Click2CollecRate.put(2, 4.816067521);
     	Click2CollecRate.put(3, 4.475648695);
     	Click2CollecRate.put(4, 4.319219643);
     	Click2CollecRate.put(5, 4.456534986);
     	Click2CollecRate.put(6, 4.176917914);
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
    	long user_item_is_click1 = 0; // 前一天是否点击过
    	long user_item_is_click2 = 0; // 前两天是否点击过
    	long user_item_is_click3 = 0;
    	long user_item_is_click4 = 0;
    	long user_item_is_click5 = 0;
    	long user_item_is_click6 = 0; // 前3小时是否点击过[21,00)
    	long user_item_is_click7 = 0; // 前6小时是否点击过[18,21)
    	long user_item_is_click8 = 0; // 前9小时是够点击过[15,18)
    	long user_item_is_click9 = 0; // 前12小时是够点击过[12,15)
    	long user_item_is_click10 = 0; // 前15小时是够点击过[9,12)
    	long user_item_is_click11 = 0; // 前18小时是够点击过[6,9)
    	long user_item_is_click12 = 0; // 前21小时是够点击过[3,6)
    	long user_item_is_click13 = 0; // 前24小时是够点击过[0,3)

    	long user_item_is_cart1 = 0; // 前一天是否点加购
    	long user_item_is_cart2 = 0; // 前两天是否点加购
    	long user_item_is_cart3 = 0;
    	long user_item_is_cart4 = 0;
    	long user_item_is_cart5 = 0;
    	long user_item_is_cart6 = 0; // 前3小时是否点加购[21,00)
    	long user_item_is_cart7 = 0; // 前6小时是否点加购[18,21)
    	long user_item_is_cart8 = 0; // 前9小时是够点加购[15,18)
    	long user_item_is_cart9 = 0; // 前12小时是够点加购[12,15)
    	long user_item_is_cart10 = 0; // 前15小时是够点加购[9,12)
    	long user_item_is_cart11 = 0; // 前18小时是够点加购[6,9)
    	long user_item_is_cart12 = 0; // 前21小时是够点加购[3,6)
    	long user_item_is_cart13 = 0; // 前24小时是够点加购[0,3)
    	
    	long user_item_is_colle1 = 0; // 前一天是否点收藏
    	long user_item_is_colle2 = 0; // 前两天是否点收藏
    	long user_item_is_colle3 = 0;
    	long user_item_is_colle4 = 0;
    	long user_item_is_colle5 = 0;
    	long user_item_is_colle6 = 0; // 前3小时是否点收藏[21,00)
    	long user_item_is_colle7 = 0; // 前6小时是否点收藏[18,21)
    	long user_item_is_colle8 = 0; // 前9小时是够点收藏[15,18)
    	long user_item_is_colle9 = 0; // 前12小时是够点收藏[12,15)
    	long user_item_is_colle10 = 0; // 前15小时是够点收藏[9,12)
    	long user_item_is_colle11 = 0; // 前18小时是够点收藏[6,9)
    	long user_item_is_colle12 = 0; // 前21小时是够点收藏[3,6)
    	long user_item_is_colle13 = 0; // 前24小时是够点收藏[0,3)
    	
    	double user_item_click1 = 0; //前一天点击数目，时间衰减下的点击数目
    	double user_item_click2 = 0; //前两天点击数目，包括1,2两天数目
    	double user_item_click3 = 0;
    	double user_item_click4 = 0;
    	double user_item_click5 = 0;
    	double user_item_click6 = 0;
    	
    	double user_item_cart1 = 0; //包括收藏和加购行为
    	double user_item_cart2 = 0;
    	double user_item_cart3 = 0;
    	double user_item_cart4 = 0;
    	double user_item_cart5 = 0;
    	double user_item_cart6 = 0;
    	
    	long user_item_click_gap = 32; //用户A对商品B的最近一次点击离购买日的天数
    	long user_item_buy_gap = 32; //用户A对商品B的最近一次购买离购买日的天数
    	long user_item_colle_gap = 32;
    	long user_item_cart_gap = 32;
    	
    	//TODO 这里31是否正确
    	long user_item_first_active = 32; //用户A对商品B的第一次交互离购买日的天数
    	long user_item_last_active = 32;
    	
    	long user_item_first_buy = 32; //用户A对商品B的第一次购买离购买日的天数
    	
//    	long user_item_first_last_buy = 32; //用户第一次购买到用户最近一次购买间隔天数
//    	long user_item_first_last_active = 32;
    	
    	int flag1 = 0;
    	int flag2 = 0;
    	int flag3 = 0;
    	int flag4 = 0;
    	
    	// 标记前六天
    	long user_item_is_only_click = 0; //是否只有点击行为
    	long user_item_is_only_cart = 0;
    	long user_item_is_only_colle = 0;
    	long user_item_is_only_buy = 0;
    	long user_item_is_click_buy = 0;
    	long user_item_is_click_and_cart = 0;
    	long user_item_is_click_and_colle = 0;
    	long user_item_is_click_and_cart_buy = 0;
    	long user_item_is_click_and_colle_buy = 0;
    	long user_item_is_click_and_cart_colle = 0;
    	long user_item_is_click_and_cart_colle_buy = 0;
    	
    	long user_item_active_day = 0; //总共的交互天数
    	//long user_item_active_hour = 0; //总共不同的交互小时数
    	
    	// 商品类别
    	long itemID = 0;
        boolean isOnlyBefore = false; // 只输出观测值前面的数据
    	long day;
    	int hour;
    	Map<String, Long[][]> behaviorCounter = new TreeMap<String, Long[][]>();
        while (values.hasNext()) {
            Record val = values.next();
            String time = val.getString("time");
    		itemID = Integer.parseInt(val.getString("item_category"));
    		// 求相对天数
    		day = relativeTime(time.substring(0, 10));
    		
    		// 获取小时数
    		hour = Integer.parseInt(time.substring(11, 13));
    		
    		int behavior_type = val.getBigint("behavior_type").intValue();
    		String days = String.format("%02d", day);
    		if (day > 0 && behavior_type == 1) {
    			// 用户A对商品B的最近一次点击离购买日的天数
    			if (user_item_click_gap == 32) {
    				// 第一次访问
    				user_item_click_gap = day;
    			} else if (user_item_click_gap > day) {
    				user_item_click_gap = day;
    			}
    			if (day <= 6) {
    				flag1 = 1;
    			}
    			isOnlyBefore = true;
    		} else if (day > 0 && behavior_type == 4) {
    			// 用户A对商品B的最近一次购买离购买日的天数
    			if (user_item_buy_gap == 32) {
    				// 第一次访问
    				user_item_buy_gap = day;
    			} else if (user_item_buy_gap > day) {
    				user_item_buy_gap = day;
    			}
    			
    			if (user_item_first_buy == 32) {
    				// 第一次访问
    				user_item_first_buy = day;
    			} else if (user_item_first_buy < day) {
    				user_item_first_buy = day;
    			}
    			if (day <= 6) {
    				flag4 = 1;
    			}
    			isOnlyBefore = true;
    			
    		} else if (day > 0 && behavior_type == 2) {
    			if (user_item_colle_gap == 32) {
    				// 第一次访问
    				user_item_colle_gap = day;
    			} else if (user_item_colle_gap > day) {
    				user_item_colle_gap = day;
    			}
    			if (day <= 6) {
    				flag2 = 1;
    			}
    			isOnlyBefore = true;
    		} else if (day > 0 && behavior_type == 3) {
    			if (user_item_cart_gap == 32) {
    				// 第一次访问
    				user_item_cart_gap = day;
    			} else if (user_item_cart_gap > day) {
    				user_item_cart_gap = day;
    			}
    			if (day <= 6) {
    				flag3 = 1;
    			}
    			isOnlyBefore = true;
    		}
    		
    		if (day > 0) {
    			if (user_item_first_active == 32) {
    				//用户A对商品B的第一次交互离购买日的天数
    				user_item_first_active = day;
    			} else if (user_item_first_active < day) {
    				user_item_first_active = day;
    			}
    			
    			if (user_item_last_active == 32) {
    				user_item_last_active = day;
    			} else if (user_item_last_active > day) {
    				user_item_last_active = day;
    			}
    		}
    		
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
    		
    		if (day <= 6 && day > 0) {
        		// 以时间为键存储行为，Map输出key已经是排序好的，是对字符进行排序的
        		// d意为整数 x为参数 02为长度为2位不足补0

        		if (behaviorCounter.containsKey(days)) {
        			behaviorCounter.get(days)[hour][behavior_type - 1] ++;
        		} else {
        			// 初始化数组，temp用于统计每个类别的行为数，后面counter是24个小时的temp
        			user_item_active_day ++;
        			Long [][] counter = zeroInit();
        			counter[hour][behavior_type - 1] ++;
        			behaviorCounter.put(days, counter);
        		}
    		}
    		
    		

        }
        
        
        Long [][] counter1 = behaviorCounter.get("01");
        Long [][] counter2 = behaviorCounter.get("02");
        Long [][] counter3 = behaviorCounter.get("03");
        Long [][] counter4 = behaviorCounter.get("04");
        Long [][] counter5 = behaviorCounter.get("05");
        Long [][] counter6 = behaviorCounter.get("06");
        double temp1,temp2,temp3,temp4,temp5,temp6;
        temp1 = calculateClick(1, counter1);
        temp2 = calculateClick(2, counter2);
        temp3 = calculateClick(3, counter3);
        temp4 = calculateClick(4, counter4);
        temp5 = calculateClick(5, counter5);
        temp6 = calculateClick(6, counter6);

        user_item_click1 = temp1; // 前一天点击数目
        user_item_click2 = temp1 + temp2;
        user_item_click3 = temp1 + temp2 + temp3;
        user_item_click4 = temp1 + temp2 + temp3 + temp4;
        user_item_click5 = temp1 + temp2 + temp3 + temp4 + temp5;
        user_item_click6 = temp1 + temp2 + temp3 + temp4 + temp5 + temp6;
        
        temp1 = calculateCart(1, counter1);
        temp2 = calculateCart(2, counter2);
        temp3 = calculateCart(3, counter3);
        temp4 = calculateCart(4, counter4);
        temp5 = calculateCart(5, counter5);
        temp6 = calculateCart(6, counter6);
        
        user_item_cart1 = temp1; // 前一天加购或收藏数目
        user_item_cart2 = temp1 + temp2;
        user_item_cart3 = temp1 + temp2 + temp3;
        user_item_cart4 = temp1 + temp2 + temp3 + temp4;
        user_item_cart5 = temp1 + temp2 + temp3 + temp4 + temp5;
        user_item_cart6 = temp1 + temp2 + temp3 + temp4 + temp5 + temp6;
        
//        if (user_item_first_buy - user_item_buy_gap > 0) {
//        	user_item_first_last_buy = user_item_first_buy - user_item_buy_gap;//用户第一次购买到用户最近一次购买间隔天数
//        }
        
        if (flag1 == 1 && flag2 == 0 && flag3 == 0 && flag4 == 0) {
        	user_item_is_only_click = 1;
        } else if (flag1 == 0 && flag2 == 1 && flag3 == 0 && flag4 == 0) {
        	user_item_is_only_colle = 1;
        } else if (flag1 == 0 && flag2 == 0 && flag3 == 1 && flag4 == 0) {
        	user_item_is_only_cart = 1;
        } else if (flag1 == 0 && flag2 == 0 && flag3 == 0 && flag4 == 1) {
        	user_item_is_only_buy = 1;
        } else if (flag1 == 1 && flag2 == 0 && flag3 == 0 && flag4 == 1) {
        	user_item_is_click_buy = 1;
        } else if (flag1 == 1 && flag2 == 0 && flag3 == 1 && flag4 == 0) {
        	user_item_is_click_and_cart = 1;
        } else if (flag1 == 1 && flag2 == 1 && flag3 == 0 && flag4 == 0) {
        	user_item_is_click_and_colle = 1;
        } else if (flag1 == 1 && flag2 == 0 && flag3 == 1 && flag4 == 1) {
        	user_item_is_click_and_cart_buy = 1;
        } else if (flag1 == 1 && flag2 == 1 && flag3 == 0 && flag4 == 1) {
        	user_item_is_click_and_colle_buy = 1;
        } else if (flag1 == 1 && flag2 == 1 && flag3 == 1 && flag4 == 0) {
        	user_item_is_click_and_cart_colle = 1;
        } else if (flag1 == 1 && flag2 == 1 && flag3 == 1 && flag4 == 1) {
        	user_item_is_click_and_cart_colle_buy = 1;
        }
        
        if (isOnlyBefore == true) {
            result.set(0, key.getString("user_id"));
            result.set(1, key.getString("item_id"));
            
            result.set(2, user_item_is_click1);
            result.set(3, user_item_is_click2);
            result.set(4, user_item_is_click3);
            result.set(5, user_item_is_click4);
            result.set(6, user_item_is_click5);
            result.set(7, user_item_is_click6);
            result.set(8, user_item_is_click7);
            result.set(9, user_item_is_click8);
            result.set(10, user_item_is_click9);
            result.set(11, user_item_is_click10);
            result.set(12, user_item_is_click11);
            result.set(13, user_item_is_click12);
            result.set(14, user_item_is_click13);
            
            result.set(15, user_item_is_cart1);
            result.set(16, user_item_is_cart2);
            result.set(17, user_item_is_cart3);
            result.set(18, user_item_is_cart4);
            result.set(19, user_item_is_cart5);
            result.set(20, user_item_is_cart6);
            result.set(21, user_item_is_cart7);
            result.set(22, user_item_is_cart8);
            result.set(23, user_item_is_cart9);
            result.set(24, user_item_is_cart10);
            result.set(25, user_item_is_cart11);
            result.set(26, user_item_is_cart12);
            result.set(27, user_item_is_cart13);
            
            result.set(28, user_item_is_colle1);
            result.set(29, user_item_is_colle2);
            result.set(30, user_item_is_colle3);
            result.set(31, user_item_is_colle4);
            result.set(32, user_item_is_colle5);
            result.set(33, user_item_is_colle6);
            result.set(34, user_item_is_colle7);
            result.set(35, user_item_is_colle8);
            result.set(36, user_item_is_colle9);
            result.set(37, user_item_is_colle10);
            result.set(38, user_item_is_colle11);
            result.set(39, user_item_is_colle12);
            result.set(40, user_item_is_colle13);
            
            result.set(41, user_item_click1);
            result.set(42, user_item_click2);
            result.set(43, user_item_click3);
            result.set(44, user_item_click4);
            result.set(45, user_item_click5);
            result.set(46, user_item_click6);
            
            result.set(47, user_item_cart1);
            result.set(48, user_item_cart2);
            result.set(49, user_item_cart3);
            result.set(50, user_item_cart4);
            result.set(51, user_item_cart5);
            result.set(52, user_item_cart6);
            
            
            result.set(53, user_item_click_gap);
            result.set(54, user_item_buy_gap);
            result.set(55, user_item_colle_gap);
            result.set(56, user_item_cart_gap);
            result.set(57, user_item_first_active);
            result.set(58, user_item_last_active);

            
            result.set(59, user_item_first_buy);
            
            result.set(60, user_item_is_only_click);
            result.set(61, user_item_is_only_cart);
            result.set(62, user_item_is_only_colle);
            result.set(63, user_item_is_only_buy);
            result.set(64, user_item_is_click_buy);
            result.set(65, user_item_is_click_and_cart);
            result.set(66, user_item_is_click_and_colle);
            result.set(67, user_item_is_click_and_cart_buy);
            result.set(68, user_item_is_click_and_colle_buy);
            result.set(69, user_item_is_click_and_cart_colle);
            result.set(70, user_item_is_click_and_cart_colle_buy);
            
            result.set(71, user_item_active_day);
            
            result.set(72, itemID);
            
            context.write(result);
        }

    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
