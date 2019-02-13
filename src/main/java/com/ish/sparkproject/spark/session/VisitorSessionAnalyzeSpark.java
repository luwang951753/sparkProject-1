package com.ish.sparkproject.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.ish.sparkproject.constant.Constants;
import com.ish.sparkproject.dao.ISessionDetailDAO;
import com.ish.sparkproject.dao.ISessionRandomExtractDAO;
import com.ish.sparkproject.dao.ITaskDAO;
import com.ish.sparkproject.dao.ITop10CategoryDAO;
import com.ish.sparkproject.dao.factory.DAOFactory;
import com.ish.sparkproject.domain.*;
import com.ish.sparkproject.test.MockData;
import com.ish.sparkproject.utils.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * spark分析,用户访问session
 *
 * 接收用户创建的分析任务,用户可能指定以下条件:
 * 1. 时间范围: 起始日期~结束日期
 * 2. 用户属性粒度: 性别,年龄范围,职业,城市
 * 3. 行为粒度: 多个搜索词,只要某个session中的任何一个action搜索过某个关键词.那么这个session就符合条件
 * 4. 点击品类: 多个品类,只要某个session中的任何一个action点击过某个品类.那么这个session就符合条件
 *
 * 我们的spark作业如何接受用户创建的任务？
 *
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param字段中
 *
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 *
 * 这是spark本身提供的特性
 */
public class VisitorSessionAnalyzeSpark {
	public static void main(String[] args) {
		args = new String[]{"1"};
		// 构建Spark的上下文
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName(Constants.SPARK_APP_NAME)
				.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses(new Class[]{CategorySortKey.class});

		SparkSession sparkSession = SparkSession.builder()
				.config(conf)
				.getOrCreate();

		// 实例化自定义的计数器SessionAggrStatAccumulator
		SessionAggrStatAccumulator sessionAggrStatAccumulator = new SessionAggrStatAccumulator();
		sparkSession.sparkContext().register(
				sessionAggrStatAccumulator, "sessionAggrStatAccumulator");

		// 生成模拟测试用数据
		mockData(sparkSession);

		/*
		 * 组件的实例化
		 * */
		ITaskDAO taskDAOImpl = DAOFactory.getTaskDAOImpl();


		/*
		 * 通过用户的创建task请求,及传入的参数数组,获取taskid
		 * 通过taskid获取相应的task以及JSON格式的参数集
		 * */
		Long taskid = ParamUtils.getTaskIdFromArgs(args);
		Task task = taskDAOImpl.findById(taskid);
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());


		/*
		 * 进行session粒度的数据聚合
		 * 	1. 从user_visit_action表中查询出指定日期范围内的行为数据
		 * 	2. 将行为数据按照sessionid进行分组,再将session粒度的数据与用户数据进行join,
		 * 		得到session粒度数据并包含对应的userid具体字段的数据
		 *	3. 将上一步筛选出的数据按照具体的taskParams进行条件筛选
		 *	4. 计算出各个visitLength和stepLength的占比并写入MySQL
		 * */
		JavaRDD<Row> actionRDD = getActionRDDByDateRange(sparkSession, taskParam);

		JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);
		// 持久化RDD,级别为:内存序列化存储
		sessionid2ActionRDD = sessionid2ActionRDD.persist(StorageLevel.MEMORY_ONLY_SER());

		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD =
				aggregateBySessonid(sparkSession, sessionid2ActionRDD);

		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD =
				filterSessionAndAggrStat(sessionid2FullAggrInfoRDD, taskParam, sessionAggrStatAccumulator);
		filteredSessionid2AggrInfoRDD =
				filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY_SER());
//		for (Tuple2 tuple:filteredSessionid2AggrInfoRDD.take(20)) {
//			System.out.println(tuple._2);
//		}

		JavaPairRDD<String, Row> sessionid2DetailRDD =
				getSessionid2DetailRDD(filteredSessionid2AggrInfoRDD, sessionid2ActionRDD);
		sessionid2DetailRDD = sessionid2DetailRDD.persist(StorageLevel.MEMORY_ONLY_SER());

		randomExtractSession(
				sparkSession,
				task.getTaskid(),
				filteredSessionid2AggrInfoRDD,
				sessionid2DetailRDD);

		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskid());

		// 获取Top10热门品类
		List<Tuple2<CategorySortKey, String>> top10CateGoryList =
				getTop10CateGory(taskid, sessionid2DetailRDD);

		// 获取Top10活跃session
		getTop10Session(sparkSession, taskid, sessionid2DetailRDD, top10CateGoryList);

		sparkSession.close();
	}

	// =========================================================================

	/**
	 * 生成模拟数据的方法
	 * @param sparkSession
	 */
	private static void mockData(SparkSession sparkSession){
		MockData.mock(sparkSession);
	}

	/**
	 * 按照日期范围,筛选出用户访问行为数据
	 * @param sparkSession
	 * @param taskParams
	 * @return
	 */
	private static JavaRDD<Row> getActionRDDByDateRange(
			SparkSession sparkSession, JSONObject taskParams){

		String startDate = ParamUtils.getParam(taskParams, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParams, Constants.PARAM_END_DATE);
		String sql =
				"select * "
						+ "from user_visit_action "
						+ "where date >='" + startDate + "'"
						+ "and date <='" + endDate + "'";
		Dataset<Row> actionDF = sparkSession.sql(sql);
		actionDF.show();
		return actionDF.javaRDD();
	}

	private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD){
		JavaPairRDD<String, Row>sessionid2Row = actionRDD.mapToPair(row -> {
			String sessionid = row.getString(2);
			return new Tuple2<>(sessionid, row);
		});
		return sessionid2Row;
	}

	/**
	 * 将按日期筛出的行为数据以sessionid进行分组聚合
	 * @param sparkSession
	 * @param sessionid2ActionRDD
	 * @return
	 */
	private static JavaPairRDD<String, String> aggregateBySessonid(
			SparkSession sparkSession, JavaPairRDD<String, Row> sessionid2ActionRDD){

		// 按照相同sessonid分组
		JavaPairRDD<String, Iterable<Row>> session2ActionsRDD =
				sessionid2ActionRDD.groupByKey();
//		System.out.println(sessionid2ActionRDD.count());

		// 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
		// 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
		JavaPairRDD<Long, String> userid2PartAggrInfoRDD = session2ActionsRDD.mapToPair(tuple -> {
			String sessionid = tuple._1;
			Iterator<Row> iterator = tuple._2.iterator();
			StringBuffer searchKeywordsBuffer = new StringBuffer("");
			StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

			Long userid = null;
			String searchKeyword = null;
			Long clickCategoryId = null;

			// session访问的起始和结束时间
			Date sessionStartTime = null;
			Date sessionEndTime = null;

			// session访问的步长
			int stepLength = 0;

			// 这一块代码的作用的是迭代同一个sessionid的所有行为信息
			while (iterator.hasNext()) {
				Row row = iterator.next();
				if (userid == null){
					userid = row.getLong(1);
				}

				// 原代码的NullPointException!首先要用isNullAt()进行非空判断!
				// 迭代比较一个sessionid的开始时间
				if (!row.isNullAt(4)){
					Date actionTime = DateUtils.parseTime(row.getString(4));
					if (actionTime != null){
						if(sessionStartTime == null) {
							sessionStartTime = actionTime;
						}
						if(sessionEndTime == null) {
							sessionEndTime = actionTime;
						}

						if(actionTime.before(sessionStartTime)) {
							sessionStartTime = actionTime;
						}
						if(actionTime.after(sessionEndTime)) {
							sessionEndTime = actionTime;
						}
					}
					// 计算session访问步长
					stepLength++;
				}

				if (!row.isNullAt(5)){
					searchKeyword = row.getString(5);
					searchKeywordsBuffer.append(searchKeyword + ",");
				}
				if (!row.isNullAt(6)){
					clickCategoryId = row.getLong(6);
					clickCategoryIdsBuffer.append(clickCategoryId + ",");
				}
			}
			String searchKeywords =
					StringUtils.trimComma(searchKeywordsBuffer.toString());
			String clickCategoryIds =
					StringUtils.trimComma(clickCategoryIdsBuffer.toString());
			long visitLength =
					(sessionEndTime.getTime() - sessionStartTime.getTime()) / 1000;

			// 大家思考一下
			// 我们返回的数据格式，即使<sessionid,partAggrInfo>
			// 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
			// 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
			// 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
			// 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
			// 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

			// 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
			// 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
			// 然后再直接将返回的Tuple的key设置成sessionid
			// 最后的数据格式，还是<sessionid,fullAggrInfo>

			// 聚合数据，用什么样的格式进行拼接？
			// 我们这里统一定义，使用key=value|key=value
			String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
					+ Constants.FIELD_SEARCH_KEY_WORDS + "=" + searchKeywords + "|"
					+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
					+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
					+ Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
					+ Constants.FIELD_START_TIME + "=" + DateUtils.formatDate(sessionStartTime);

			return new Tuple2<Long, String>(userid, partAggrInfo);
		});

		// 查询所有用户数据，并映射成<userid,Row>的格式
		String sql = "select * from user_info";
		JavaRDD<Row> userInfoRDD = sparkSession.sql(sql).javaRDD();
		JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(row -> {
			long userid = row.getLong(0);
			return new Tuple2<Long, Row>(userid, row);
		});

		// 将session粒度聚合数据，与用户信息进行join
		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD =
				userid2PartAggrInfoRDD.join(userid2InfoRDD);

		// 对join起来的数据进行拼接，并且返回<sessionid, fullAggrInfo>格式的数据
		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(tuple -> {
			Long userid = tuple._1;
			String partAggrInfo = tuple._2._1;
			Row userInfoRow = tuple._2._2;
			int age = userInfoRow.getInt(3);
			String professional = userInfoRow.getString(4);
			String city = userInfoRow.getString(5);
			String sex = userInfoRow.getString(6);
			String fullAggrInfo = partAggrInfo + "|"
					+ Constants.FIELD_AGE + "=" + age + "|"
					+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
					+ Constants.FIELD_CITY + "=" + city + "|"
					+ Constants.FIELD_SEX + "=" + sex;
			String sessionid = StringUtils.getFieldFromConcatString(
					partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
			return new Tuple2<String, String>(sessionid, fullAggrInfo);
		});
		return sessionid2FullAggrInfoRDD;
	}

	/**
	 * 按照用户的信息参数taskParams来过滤数据
	 * @param sessionid2FullAggrInfoRDD 按sessionid聚合并且join后的数据
	 * @param taskParam 过滤条件
	 * @return
	 */
	private static JavaPairRDD<String, String> filterSessionAndAggrStat(
			JavaPairRDD<String, String> sessionid2FullAggrInfoRDD,
			JSONObject taskParam,
			SessionAggrStatAccumulator sessionAggrStatAccumulator){

		// taskParams: JSONObject->String
		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEY_WORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

		// 重新拼接taskParams
		String concatParm = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_KEY_WORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");
		if (concatParm.trim().endsWith("|")){
			concatParm = concatParm.trim().substring(0, concatParm.length() - 1);
		}
		String parameter = concatParm;

		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2FullAggrInfoRDD.filter(tuple ->{
			// 1.获取对应sessionid的全部用户信息
			String fullAggrInfo = tuple._2;
			/*
			*  2. 按照年龄范围(startAge~endAge)进行过滤
			*  ValidUtils.between(fullAggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)
			*  	先调用了getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_AGE)得到age字段的值
			*  	然后调用getFieldFromConcatString(parameter, "\\|", Constants.PARAM_START_AGE)得到startAge参数的值
			*  	再调用getFieldFromConcatString(parameter, "\\|", Constants.PARAM_END_AGE)得到endAge参数的值
			*  	最后获取比较结果
			 */

			// 年龄范围过滤
			if (!ValidUtils.between(fullAggrInfo, Constants.FIELD_AGE,
					parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)){
				return false;
			}

			// 职业过滤
			if (!ValidUtils.in(fullAggrInfo, Constants.FIELD_PROFESSIONAL,
					parameter, Constants.PARAM_PROFESSIONALS)){
				return false;
			}

			// 城市过滤
			if(!ValidUtils.in(fullAggrInfo, Constants.FIELD_CITY,
					parameter, Constants.PARAM_CITIES)) {
				return false;
			}

			// 性别过滤
			if(!ValidUtils.equal(fullAggrInfo, Constants.FIELD_SEX,
					parameter, Constants.PARAM_SEX)) {
				return false;
			}

			// 按照搜索词进行过滤
			if(!ValidUtils.in(fullAggrInfo, Constants.FIELD_SEARCH_KEY_WORDS,
					parameter, Constants.PARAM_KEY_WORDS)) {
				return false;
			}

			// 按照点击品类id进行过滤
			if(!ValidUtils.in(fullAggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
					parameter, Constants.PARAM_CATEGORY_IDS)) {
				return false;
			}

			sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
			Long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
					fullAggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH)
			);

			Long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
					fullAggrInfo, "\\|", Constants.FIELD_STEP_LENGTH)
			);

			calculateVisitLength(visitLength, sessionAggrStatAccumulator);
			calculateStepLength(stepLength, sessionAggrStatAccumulator);

			return true;
		});

		return filteredSessionid2AggrInfoRDD;
	}


	/**
	 * 通过筛选条件的session访问条件的明细数据RDD
	 * @param filteredSessionid2AggrInfoRDD
	 * @param sessionid2Row
	 * @return
	 */
	private static JavaPairRDD<String, Row> getSessionid2DetailRDD(
			JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2Row){

		JavaPairRDD<String, Row> sessionid2detailRDD =
				filteredSessionid2AggrInfoRDD.join(sessionid2Row).mapToPair(tuple -> {
					return new Tuple2<>(tuple._1, tuple._2._2);
				});
		return sessionid2detailRDD;
	}

	/**
	 * 对 访问时长visitLength进行分段计数统计
	 * @param visitLength
	 * @param sessionAggrStatAccumulator
	 */
	private static void calculateVisitLength(
			Long visitLength, SessionAggrStatAccumulator sessionAggrStatAccumulator){
		if (visitLength >= 1 && visitLength<=3){
			sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
		}else if (visitLength>=4 && visitLength<=6){
			sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
		}else if (visitLength>=7 && visitLength<=9){
			sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
		}else if (visitLength>=10 && visitLength<=30){
			sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
		}else if (visitLength>30 && visitLength<=60){
			sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
		}else if (visitLength>60 && visitLength<=180){
			sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
		}else if (visitLength>180 && visitLength<=600){
			sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
		}else if (visitLength>600 && visitLength<=1800){
			sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
		}else if (visitLength>1800){
			sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
		}
	}

	/**
	 * 对 访问步长stepLength进行分段计数统计
	 * @param stepLength
	 * @param sessionAggrStatAccumulator
	 */
	private static void calculateStepLength(
			Long stepLength, SessionAggrStatAccumulator sessionAggrStatAccumulator){
		if (stepLength>=1 && stepLength<=3){
			sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
		}else if (stepLength>=4 && stepLength<=6){
			sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
		}else if (stepLength>=7 && stepLength<=9){
			sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
		}else if (stepLength>=10 && stepLength<=30){
			sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
		}else if (stepLength>=30 && stepLength<=60){
			sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
		}else if (stepLength>60){
			sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
		}
	}


	/**
	 * 按照每天各个时段的session占比,随机抽取1000个session
	 * @param filteredSessionid2AggrInfoRDD
	 */
	private static void randomExtractSession(
			SparkSession sparkSession,
			long taskid,
			JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2DetailRDD){

		// 计算出每天每小时yyyy-MM-dd_HH的session数量 <yyyy-MM-dd_HH, partAggrInfo>
		JavaPairRDD<String, String> time2SessionidRDD = filteredSessionid2AggrInfoRDD.mapToPair(tuple-> {
			String partAggrInfo = tuple._2;
			String startTime = StringUtils.getFieldFromConcatString(
					partAggrInfo, "\\|", Constants.FIELD_START_TIME);
			String dateHour = DateUtils.getDateHour(startTime);
			return new Tuple2<String, String>(dateHour, partAggrInfo);
		});

		// 按照session的时间进行聚合得到每个时间段的session数量
		Map<String, Long> hourCountMap = time2SessionidRDD.countByKey();

		// 将<yyyy-MM-dd_HH,count>转换为格式<yyyy-MM-dd,<HH,count>>
		Map<String,Map<String, Long>> dayHourCountMap =
				new HashMap<String,Map<String, Long>>();
		for (Map.Entry<String, Long> countEntry : hourCountMap.entrySet()) {
			String date = countEntry.getKey().split("_")[0];
			String hour = countEntry.getKey().split("_")[1];
			Long count = countEntry.getValue();
			Map<String, Long> hourCount = dayHourCountMap.get(date);

			if (hourCount == null){
				hourCount = new HashMap<String, Long>();
				dayHourCountMap.put(date, hourCount);
			}
			hourCount.put(hour, count);
		}

		/*
		按每天各个时段比例的随机抽取算法
		* */
		// 按天数平均划分收取数量
		int extractNumberPerDay = 15 / dayHourCountMap.size();

		// 初始化map,存放每天每小时抽取session索引的数据, 数据格式<date,<hour,(3,5,...,34)>>
		Map<String, Map<String, List<Integer>>> dateHourExtractMap =
				new HashMap<String, Map<String, List<Integer>>>();

		Random random = new Random();

		// 遍历 <每天,<每小时, session>>数据 Map<2018-09-10, Map<01,123>>
		for (Map.Entry<String,Map<String, Long>> countEntry : dayHourCountMap.entrySet()) {
			String date = countEntry.getKey();
			Map<String, Long> hourCount = countEntry.getValue();

			// 每天的session总数
			long totalSessionPerday = 0;
			for (Long sessionPerHour : hourCount.values()) {
				totalSessionPerday += sessionPerHour;
			}

			// 初始化list存放对应每小时的的session的抽取随机数
			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
			if (hourExtractMap == null){
				hourExtractMap = new HashMap<String, List<Integer>>();
				dateHourExtractMap.put(date, hourExtractMap);
			}

			// 遍历 <每小时,session>数据	Map<hour, sessionCount>
			for (Map.Entry<String, Long> hourSessionCountEntry : hourCount.entrySet()) {
				String hour = hourSessionCountEntry.getKey();
				long sessionCount = hourSessionCountEntry.getValue();
				int sessionExtractNumPerHour =
						(int)(((double) sessionCount / (double) totalSessionPerday) * extractNumberPerDay);
				if (sessionExtractNumPerHour > totalSessionPerday){
					sessionExtractNumPerHour = (int)totalSessionPerday;
				}

				List<Integer> extractIndexList = hourExtractMap.get(hour);
				if (extractIndexList == null){
					extractIndexList = new ArrayList<Integer>();
					hourExtractMap.put(hour, extractIndexList);
				}

				// 将按照比例每天每小时抽取的session数量 生成随机数
				for (int i = 0; i < sessionExtractNumPerHour; i++) {
					int extractIndex = random.nextInt((int)sessionCount);
					while (extractIndexList.contains(extractIndex)){
						extractIndex = random.nextInt((int)sessionCount);
					}
					extractIndexList.add(extractIndex);
				}
			}
		}

		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
		final Broadcast<Map<String, Map<String, List<Integer>>>> dateHourExtractMapBroadcast =
				jsc.broadcast(dateHourExtractMap);

		/*
		* 遍历每天每小时的session,根据随机索引进行抽取
		* 并将符合随机筛选条件的记录写入session_random_extract表中
		* */
		// <yyyy-MM-dd_HH, Iterable<partAggrInfo>>
		JavaPairRDD<String, Iterable<String>> time2sessionidsRDD = time2SessionidRDD.groupByKey();

		JavaPairRDD<String, String> extractSessionidsRDD = time2sessionidsRDD.flatMapToPair(tuple->{
			List<Tuple2<String, String>> extractSessionids = new ArrayList<>();
			String dateHour = tuple._1;
			String date = dateHour.split("_")[0];
			String hour = dateHour.split("_")[1];
			Iterator<String> iterator = tuple._2.iterator();

			Map<String, Map<String, List<Integer>>> dateHourExtractMapBroadcastValue = dateHourExtractMapBroadcast.getValue();
			List<Integer> extractIndexList = dateHourExtractMapBroadcastValue.get(date).get(hour);

			ISessionRandomExtractDAO sessionRandomExtractDAOImpl =
					DAOFactory.getSessionRandomExtractDAOImpl();
			int index = 0;
			while (iterator.hasNext()){
				String partAggrInfo = iterator.next();
				if (extractIndexList.contains(index)) {
					String sessionid = StringUtils.getFieldFromConcatString(
							partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
					SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
					sessionRandomExtract.setTaskid(taskid);
					sessionRandomExtract.setSessionid(sessionid);
					sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
							partAggrInfo, "\\|", Constants.FIELD_START_TIME));
					sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
							partAggrInfo, "\\|", Constants.FIELD_SEARCH_KEY_WORDS));
					sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
							partAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

					sessionRandomExtractDAOImpl.insert(sessionRandomExtract);
					extractSessionids.add(new Tuple2<>(sessionid, sessionid));
				}
				index++;
			}
			return extractSessionids.iterator();
		});

		/*
		* 将上面格式好的随机抽取出的extractSessionidsRDD:<sessionid, sessionidid>和sessionid2Row:<sessionid, Row>
		*     进行join就可以得到extractSessionidsRDD的每个session的明细数据,即extractSessionDetailRDD
		* */
		JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
				extractSessionidsRDD.join(sessionid2DetailRDD);

		SessionDetail sessionDetail = new SessionDetail();
		ISessionDetailDAO sessionDetailDAOImpl = DAOFactory.getSessionDetailDAOImpl();
		extractSessionDetailRDD.foreach(tuple->{
			Row row = tuple._2._2;
			sessionDetail.setTaskid(taskid);
			sessionDetail.setUserid(row.getLong(1));
			sessionDetail.setSessionid(row.getString(2));
			sessionDetail.setPageid(row.getLong(3));
			sessionDetail.setActionTime(row.getString(4));
			sessionDetail.setSearchKeyword(row.getString(5));
			sessionDetail.setClickCategoryId(row.getLong(6));
			sessionDetail.setClickProductId(row.getLong(7));
			sessionDetail.setOrderCategoryIds(row.getString(8));
			sessionDetail.setOrderProductIds(row.getString(9));
			sessionDetail.setPayCategoryIds(row.getString(10));
			sessionDetail.setPayProductIds(row.getString(11));

			sessionDetailDAOImpl.insert(sessionDetail);
		});

	}


	/**
	 * 计算visitLength和stepLength的各个范围的占比
	 * 	并将各个范围封装为SessionAggrStat对象调动insert(),写入MySQL!
	 * @param value
	 */
	private static void calculateAndPersistAggrStat(String value, Long taskid){
		System.out.println(value);

		Long sessionCount = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
		Long visitLength_1s_3s = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
		Long visitLength_4s_6s = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
		Long visitLength_7s_9s = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
		Long visitLength_10s_30s = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
		Long visitLength_30s_60s = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
		Long visitLength_1m_3m = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
		Long visitLength_3m_10m = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
		Long visitLength_10m_30m = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
		Long visitLength_30m = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

		Long stepLength_1_3 = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
		Long stepLength_4_6 = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
		Long stepLength_7_9 = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
		Long stepLength_10_30 = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
		Long stepLength_30_60 = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
		Long stepLength_60 = Long.valueOf(
				StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

		double visitLength_1s_3s_ratio = NumberUtils.formatDouble(
				(double)visitLength_1s_3s / (double)sessionCount,2);
		double visitLength_4s_6s_tatio = NumberUtils.formatDouble(
				(double)visitLength_4s_6s / (double)sessionCount,2);
		double visitLength_7s_9s_ratio = NumberUtils.formatDouble(
				(double)visitLength_7s_9s / (double)sessionCount,2);
		double visitLength_10s_30s_ratio = NumberUtils.formatDouble(
				(double)visitLength_10s_30s / (double)sessionCount,2);
		double visitLength_30s_60s_ratio = NumberUtils.formatDouble(
				(double)visitLength_30s_60s / (double)sessionCount,2);
		double visitLength_1m_3m_ratio = NumberUtils.formatDouble(
				(double)visitLength_1m_3m / (double)sessionCount,2);
		double visitLength_3m_10m_ratio = NumberUtils.formatDouble(
				(double)visitLength_3m_10m / (double)sessionCount,2);
		double visitLength_10m_30m_ratio = NumberUtils.formatDouble(
				(double)visitLength_10m_30m / (double)sessionCount,2);
		double visitLength_30m_ratio = NumberUtils.formatDouble(
				(double)visitLength_30m / (double)sessionCount,2);

		double stepLength_1_3_ratio = NumberUtils.formatDouble(
				(double)stepLength_1_3 / (double)sessionCount,2);
		double stepLength_4_6_ratio = NumberUtils.formatDouble(
				(double)stepLength_4_6 / (double)sessionCount,2);
		double stepLength_7_9_ratio = NumberUtils.formatDouble(
				(double)stepLength_7_9 / (double)sessionCount,2);
		double stepLength_10_30_ratio = NumberUtils.formatDouble(
				(double)stepLength_10_30 / (double)sessionCount,2);
		double stepLength_30_60_ratio = NumberUtils.formatDouble(
				(double)stepLength_30_60 / (double)sessionCount,2);
		double stepLength_60_ratio = NumberUtils.formatDouble(
				(double)stepLength_60 / (double)sessionCount,2);

		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskid);
		sessionAggrStat.setSession_count(sessionCount);
		sessionAggrStat.setVisitLength_1s_3s(visitLength_1s_3s_ratio);
		sessionAggrStat.setVisitLength_4s_6s(visitLength_4s_6s_tatio);
		sessionAggrStat.setVisitLength_7s_9s(visitLength_7s_9s_ratio);
		sessionAggrStat.setVisitLength_10s_30s(visitLength_10s_30s_ratio);
		sessionAggrStat.setVisitLength_30s_60s(visitLength_30s_60s_ratio);
		sessionAggrStat.setVisitLength_1m_3m(visitLength_1m_3m_ratio);
		sessionAggrStat.setVisitLength_3m_10m(visitLength_3m_10m_ratio);
		sessionAggrStat.setVisitLength_10m_30m(visitLength_10m_30m_ratio);
		sessionAggrStat.setVisitLength_30m(visitLength_30m_ratio);
		sessionAggrStat.setStepLength_1_3(stepLength_1_3_ratio);
		sessionAggrStat.setStepLength_4_6(stepLength_4_6_ratio);
		sessionAggrStat.setStepLength_7_9(stepLength_7_9_ratio);
		sessionAggrStat.setStepLength_10_30(stepLength_10_30_ratio);
		sessionAggrStat.setStepLength_30_60(stepLength_30_60_ratio);
		sessionAggrStat.setStepLength_60(stepLength_60_ratio);

		System.out.println(sessionAggrStat.toString());

		// 将数据插入MySQL数据库表
		DAOFactory.getSessionAggrStatDAOImpl().insert(sessionAggrStat);
	}

	/**
	 * 获取Top10热门品类,并将结果写入MySQL
	 * @param sessionid2DetailRDD
	 */
	private static List<Tuple2<CategorySortKey, String>> getTop10CateGory(
			long taskid,
			JavaPairRDD<String, Row> sessionid2DetailRDD){

		// 获取所有的发生用户行为的品类ID
		JavaPairRDD<Long, Long> categoryIdRDD = sessionid2DetailRDD.flatMapToPair(tuple -> {
			String sessionid = tuple._1;
			Row row = tuple._2;
			List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
			if (!row.isNullAt(6)) {
				long clickCategoryId = row.getLong(6);
				list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
			}
			if (!row.isNullAt(8)) {
				String orderCategoryIds = row.getString(8);
				String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
				for (String orderCategoryId : orderCategoryIdsSplited) {
					list.add(new Tuple2<Long, Long>(
							Long.valueOf(orderCategoryId),
							Long.valueOf(orderCategoryId)));
				}
			}
			if (!row.isNullAt(10)) {
				String payCategoryIds = row.getString(10);
				String[] payCategoryIdsSplited = payCategoryIds.split(",");
				for (String payCategoryId : payCategoryIdsSplited) {
					list.add(new Tuple2<Long, Long>(
							Long.valueOf(payCategoryId),
							Long.valueOf(payCategoryId)));
				}
			}
			return list.iterator();
		});

		// categoryIdRDD去重
		categoryIdRDD = categoryIdRDD.distinct();

		// 计算各个品类的点击次数
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD =
				getClickCategoryId2CountRDD(sessionid2DetailRDD);

		// 计算各个品类的下单次数
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD =
				getOrderCategoryId2CountRDD(sessionid2DetailRDD);

		// 计算各个品类的支付次数
		JavaPairRDD<Long, Long> payCategoryId2CountRDD =
				getPayCategoryId2CountRDD(sessionid2DetailRDD);

		// 获得各个品类的categoryId 和 count
		// 数据格式 <001, categoryId=001|clickCount=2314|orderCount=324|payCount=124>
		JavaPairRDD<Long, String> categoryId2CountRDD = joinCategoryAndData(
				categoryIdRDD,
				clickCategoryId2CountRDD,
				orderCategoryId2CountRDD,
				payCategoryId2CountRDD);

		JavaPairRDD<CategorySortKey, String> sortKey2CountRDD = categoryId2CountRDD.mapToPair(tuple -> {
			String countInfo = tuple._2;
			long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "|", Constants.FIELD_CLICK_COUNT));
			long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "|", Constants.FIELD_ORDER_COUNT));
			long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "|", Constants.FIELD_PAY_COUNT));

			CategorySortKey sortKey = new CategorySortKey(clickCount, orderCount, payCount);
			return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
		});

		// 使用自定义排序键类CategorySortKey的排序方法
		// 数据格式Tuple2<CategorySortKey, String>(sortKey, countInfo)
		JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD =
				sortKey2CountRDD.sortByKey(false);

		List<Tuple2<CategorySortKey, String>> top10CategoryList =
				sortedCategoryCountRDD.take(10);

		ITop10CategoryDAO top10CategoryImpl = DAOFactory.getTop10CategoryImpl();

		for (Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
			String countInfo = tuple._2;
			long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "|", Constants.FIELD_CATEGORY_ID
			));
			long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "|", Constants.FIELD_CLICK_COUNT
			));
			long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "|", Constants.FIELD_ORDER_COUNT
			));
			long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "|", Constants.FIELD_PAY_COUNT
			));

			Top10Category top10Category = new Top10Category();
			top10Category.setTaskid(taskid);
			top10Category.setCategoryid(categoryid);
			top10Category.setClickCount(clickCount);
			top10Category.setOrderCount(orderCount);
			top10Category.setPayCount(payCount);

			top10CategoryImpl.insert(top10Category);
		}
		return top10CategoryList;
	}

	/**
	 * 获取各个品类的点击次数
	 * @param session2detailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(
			JavaPairRDD<String, Row> session2detailRDD){

		JavaPairRDD<String, Row> clickActionRDD = session2detailRDD.filter(tuple -> {
			Row row = tuple._2;
			return !row.isNullAt(6) ? true : false;
		});
		JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(tuple -> {
			long clickCategoryId = tuple._2.getLong(6);
			return new Tuple2<>(clickCategoryId, 1L);
		});
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
				(v1,v2) -> v1 + v2
		);

		return clickCategoryId2CountRDD;
	}

	/**
	 * 获取各个品类的下单次数
	 * @param session2detailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(
			JavaPairRDD<String, Row> session2detailRDD){
		JavaPairRDD<String, Row> orderActionRDD = session2detailRDD.filter(tuple -> {
			Row row = tuple._2;
			return !row.isNullAt(8) ? true : false;
		});
		JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(tuple -> {
			String orderCategoryIds = tuple._2.getString(8);
			String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
			List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
			for (String orderCategoryId : orderCategoryIdsSplited) {
				list.add(new Tuple2<>(Long.valueOf(orderCategoryId), 1L));
			}
			return list.iterator();
		});
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
				(v1,v2) -> v1 + v2
		);
		return orderCategoryId2CountRDD;
	}

	/**
	 * 获取各个品类的支付次数
	 * @param session2detailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(
			JavaPairRDD<String, Row> session2detailRDD){
		JavaPairRDD<String, Row> payActionRDD = session2detailRDD.filter(tuple -> {
			Row row = tuple._2;
			return !row.isNullAt(10) ? true : false;
		});
		JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(tuple -> {
			String payCategoryIds = tuple._2.getString(10);
			String[] payCategoryIdsSplited = payCategoryIds.split(",");
			List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
			for (String payCategoryId : payCategoryIdsSplited) {
				list.add(new Tuple2<>(Long.valueOf(payCategoryId), 1L));
			}
			return list.iterator();
		});
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
				(v1,v2) -> v1 + v2
		);
		return payCategoryId2CountRDD;
	}

	/**
	 * 将点击,下单,支付的统计结果和各自ID组合成可排序数据格式
	 * @param categoryIdRDD
	 * @param clickCategoryId2CountRDD
	 * @param orderCategoryId2CountRDD
	 * @param payCategoryId2CountRDD
	 * @return
	 */
	private static JavaPairRDD<Long, String> joinCategoryAndData(
			JavaPairRDD<Long, Long> categoryIdRDD,
			JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
			JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
			JavaPairRDD<Long, Long> payCategoryId2CountRDD){

		/*
		* leftOuterJoin时右边的数据可能为空值
		* 所以Tuple2<Long, Optional<Long>>中的第二个为Optional<Long>
		*/
		JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinClickRDD =
				categoryIdRDD.leftOuterJoin(clickCategoryId2CountRDD);

		JavaPairRDD<Long, String> tmpMapRDD =
				tmpJoinClickRDD.mapToPair(tuple -> {
			long categoryId = tuple._1;
			Optional<Long> clickCountOptional = tuple._2._2;
			long clickCount = 0L;
			if (clickCountOptional.isPresent()) {
				clickCount = clickCountOptional.get();
			}
			String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|"
					+ Constants.FIELD_CLICK_COUNT + "=" + clickCount;
			return new Tuple2<Long, String>(categoryId, value);
		});

		JavaPairRDD<Long, Tuple2<String, Optional<Long>>> tmpJoinOrderRDD =
				tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD);

		tmpMapRDD = tmpJoinOrderRDD.mapToPair(tuple ->{
			long categoryId = tuple._1;
			Optional<Long> orderCountOptional = tuple._2._2;
			long orderCount = 0L;
			if (orderCountOptional.isPresent()){
				orderCount = orderCountOptional.get();
			}
			String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|"
					+ Constants.FIELD_ORDER_COUNT + "=" + orderCount;
			return new Tuple2<Long, String>(categoryId, value);
		});

		JavaPairRDD<Long, Tuple2<String, Optional<Long>>> tmpJoinPayRDD =
				tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD);

		tmpMapRDD = tmpJoinPayRDD.mapToPair(tuple -> {
			long categoryId = tuple._1;
			Optional<Long> payCountOptional = tuple._2._2;
			long payCount = 0L;
			if (payCountOptional.isPresent()) {
				payCount = payCountOptional.get();
			}
			String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|"
					+ Constants.FIELD_PAY_COUNT + "=" + payCount;
			return new Tuple2<Long, String>(categoryId, value);
		});

		return tmpMapRDD;
	}

	/**
	 * 获取Top10热门活跃session
	 * @param taskid
	 * @param sessionid2DetailRDD
	 */
	private static void getTop10Session(
			SparkSession sparkSession,
			long taskid,
			JavaPairRDD<String, Row> sessionid2DetailRDD,
			List<Tuple2<CategorySortKey, String>> top10CategoryList){

		/*
		* 第一步: 将Top10热门品类的id生成一份JavaPairRDD<Long, Long>(categoryId, categoryId)
		* */
		List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<Tuple2<Long, Long>>();
		for (Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
			String countInfo = tuple._2;
			long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "|", Constants.FIELD_CATEGORY_ID));
			top10CategoryIdList.add(new Tuple2<Long, Long>(categoryId, categoryId));
		}
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
		JavaPairRDD<Long, Long> top10CategoryIdRDD = jsc.parallelizePairs(top10CategoryIdList);

		/*
		* 第二步: 计算Top10品类被各个session点击的次数
		* */
		JavaPairRDD<String, Iterable<Row>> sessionid2DetailsRDD =
				sessionid2DetailRDD.groupByKey();

		JavaPairRDD<Long, String> categoryId2SessionCountRDD = sessionid2DetailsRDD.flatMapToPair(tuple -> {
			String sessionid = tuple._1;
			Iterator<Row> iterator = tuple._2.iterator();
			// 获取数据格式Map<Long, Long>(categoryId, count)
			Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
			while (iterator.hasNext()) {
				Row row = iterator.next();
				if (!row.isNullAt(6)) {
					long categoryId = row.getLong(6);
					Long count = categoryCountMap.get(categoryId);
					if (count == null) {
						count = 0L;
					}
					count++;
					categoryCountMap.put(categoryId, count);
				}
			}

			// 返回出的数据格式List<Tuple2<Long, String>>(categoryId, (sessionid,count))
			List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
			for (Map.Entry<Long, Long> entry : categoryCountMap.entrySet()) {
				long categoryId = entry.getKey();
				long count = entry.getValue();
				String value = sessionid + "," + count;
				list.add(new Tuple2<Long, String>(categoryId, value));
			}
			return list.iterator();
		});

		// JavaPairRDD<Long, Long>(categoryId, categoryId)和
		// JavaPairRDD<Long, String>(categoryId, (sessionid,count))进行join
		// 返回结果: JavaPairRDD<Long, String>(categoryId, (sessionid,count))
		JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD
				.join(categoryId2SessionCountRDD)
				.mapToPair(tuple -> {
					return new Tuple2<>(tuple._1, tuple._2._2);
				});

		/*
		* 第三步: 分组取TopN算法的实现,获取每个品类的top10活跃用户
		* */
		JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =
				top10CategorySessionCountRDD.groupByKey();

		JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(tuple -> {
			long categoryId = tuple._1;
			Iterator<String> iterator = tuple._2.iterator();

			String[] top10Sessions = new String[10];
			while (iterator.hasNext()) {
				String sessionCount = iterator.next();
				String sessionid = sessionCount.split(",")[0];
				long count = Long.valueOf(sessionCount.split(",")[1]);
				for (int i = 0; i < top10Sessions.length; i++) {
					if (top10Sessions[i] == null) {
						top10Sessions[i] = sessionCount;
						break;
					}else {
						long _count = Long.valueOf(top10Sessions[i].split(",")[1]);
						if (count > _count){
							for (int j = 9; j > i ; j--) {
								top10Sessions[j] = top10Sessions[j-1];
							}
							top10Sessions[i] = sessionCount;
							break;
						}
					}
				}
			}
			List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
			for (String sessionCount : top10Sessions) {
				if (sessionCount != null){
					String sessionid = sessionCount.split(",")[0];
					long count = Long.valueOf(sessionCount.split(",")[1]);
					Top10Session top10Session = new Top10Session();
					top10Session.setTaskid(taskid);
					top10Session.setCategoryid(categoryId);
					top10Session.setSessionid(sessionid);
					top10Session.setClickCount(count);
					DAOFactory.getTop10SessionDAOImpl().insert(top10Session);
					list.add(new Tuple2<>(sessionid, sessionCount));
				}
			}
			return list.iterator();
		});

		/*
		* 第四步: 获取Top10活跃用户的明细数据
		* */
		// (sessionid,(sessionCount, Row))
		JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
				top10SessionRDD.join(sessionid2DetailRDD);
		sessionDetailRDD.foreach(tuple -> {
			String sessionid = tuple._1;
			Row row = tuple._2._2;
			SessionDetail sessionDetail = new SessionDetail();
			sessionDetail.setTaskid(taskid);
			sessionDetail.setUserid(row.getLong(1));
			sessionDetail.setSessionid(row.getString(2));
			sessionDetail.setPageid(row.getLong(3));
			sessionDetail.setActionTime(row.getString(4));
			sessionDetail.setSearchKeyword(row.getString(5));
			sessionDetail.setClickCategoryId(row.getLong(6));
			sessionDetail.setClickProductId(row.getLong(7));
			sessionDetail.setOrderCategoryIds(row.getString(8));
			sessionDetail.setOrderProductIds(row.getString(9));
			sessionDetail.setPayCategoryIds(row.getString(10));
			sessionDetail.setPayProductIds(row.getString(11));

			ISessionDetailDAO sessionDetailDAOImpl = DAOFactory.getSessionDetailDAOImpl();
			sessionDetailDAOImpl.insert(sessionDetail);
		});
	}

}
