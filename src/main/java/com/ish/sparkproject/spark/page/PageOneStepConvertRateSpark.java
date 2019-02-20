package com.ish.sparkproject.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.ish.sparkproject.constant.Constants;
import com.ish.sparkproject.dao.IPageSplitConvertRateDAO;
import com.ish.sparkproject.dao.ITaskDAO;
import com.ish.sparkproject.dao.factory.DAOFactory;
import com.ish.sparkproject.domain.PageSplitConvertRate;
import com.ish.sparkproject.domain.Task;
import com.ish.sparkproject.utils.DateUtils;
import com.ish.sparkproject.utils.NumberUtils;
import com.ish.sparkproject.utils.ParamUtils;
import com.ish.sparkproject.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class PageOneStepConvertRateSpark {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_PAGE)
				.setMaster("local[2]");
		SparkSession sparkSession = SparkSession.builder()
				.config(conf)
				.getOrCreate();

		// 生成模拟数据
		SparkUtils.mockData(sparkSession);

		// 查询任务,获取任务参数
		long taskid = ParamUtils.getTaskIdFromArgs(args);

		ITaskDAO taskDAOImpl = DAOFactory.getTaskDAOImpl();
		Task task = taskDAOImpl.findById(taskid);

		if (task == null){
			System.out.println(new Date() + ": can not find this taskid: [" + taskid + "].");
			return;
		}
		JSONObject taskParams = JSONObject.parseObject(task.getTaskParam());

		// 获取指定日期内的用户访问行为数据
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sparkSession, taskParams);
		JavaPairRDD<String, Row> sessionid2ActionRDD = SparkUtils.getSessionid2ActionRDD(actionRDD);
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();
		sessionid2ActionsRDD.cache();

		// 每个sessionid的单跳页面切片的生成以及页面流的匹配
		JavaPairRDD<String, Integer> pageSplitAndOne =
				genAndMatchPageSplit(sparkSession, sessionid2ActionsRDD, taskParams);
		Map<String, Long> pageSplitPvMap = pageSplitAndOne.countByKey();
		long startPagePV = getStartPagePV(taskParams, sessionid2ActionsRDD);

		// 计算目标流中的页面单跳率
		Map<String, Double> convertRateMap =
				computePageSplitConvertRate(taskParams, pageSplitPvMap, startPagePV);

		// 计算数据持久化到数据库
		persistConvertRate(taskid, convertRateMap);

		sparkSession.close();
	}

	/**
	 * 页面切片生成与匹配的算法
	 * @param sparkSession
	 * @param sessionid2ActionsRDD
	 * @param taskparams
	 * @return
	 */
	private static JavaPairRDD<String, Integer> genAndMatchPageSplit(
			SparkSession sparkSession,
			JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD,
			JSONObject taskparams){

		String targetPageFlow = ParamUtils.getParam(taskparams, Constants.PARAM_TARGET_PAGE_FLOW);
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
		Broadcast<String> targetPageFlowBroadcast = jsc.broadcast(targetPageFlow);

		JavaPairRDD<String, Integer> pageSplitAndOne = sessionid2ActionsRDD.flatMapToPair(tuple -> {
			List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
			Iterator<Row> iterator = tuple._2.iterator();
			String[] targetPages = targetPageFlowBroadcast.value().split(",");

			List<Row> rows = new ArrayList<>();
			while (iterator.hasNext()) {
				rows.add(iterator.next());
			}
			Collections.sort(rows, new Comparator<Row>() {
				@Override
				public int compare(Row o1, Row o2) {
					String actionTime1 = o1.getString(4);
					String actionTime2 = o2.getString(4);

					Date date1 = DateUtils.parseTime(actionTime1);
					Date date2 = DateUtils.parseTime(actionTime2);

					return (int) (date1.getTime() - date2.getTime());
				}
			});

			// 生成页面切片以及页面流匹配
			Long lastPageid = null;
			for (Row row : rows) {
				long pageid = row.getLong(3);
				if (lastPageid == null) {
					lastPageid = pageid;
					continue;
				}
				String pageSplit = lastPageid + "_" + pageid;
				for (int i = 1; i < targetPages.length; i++) {
					String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
					if (pageSplit.equals(targetPageSplit)) {
						list.add(new Tuple2<String, Integer>(pageSplit, 1));
						break;
					}
				}
				lastPageid = pageid;
			}
			return list.iterator();
		});
		return pageSplitAndOne;
	}

	/**
	 * 获取页面流中初始页面的PV
	 * @param taskparams
	 * @param sessionid2ActionsRDD
	 * @return
	 */
	private static long getStartPagePV(
			JSONObject taskparams,
			JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD){

		String targetPageFlow = ParamUtils.getParam(taskparams, Constants.PARAM_TARGET_PAGE_FLOW);
		long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);

		JavaRDD<Long> startPageRDD = sessionid2ActionsRDD.flatMap(tuple -> {
			Iterator<Row> iterator = tuple._2.iterator();
			List<Long> list = new ArrayList<>();
			while (iterator.hasNext()) {
				Row row = iterator.next();
				long pageId = row.getLong(3);
				if (pageId == startPageId) {
					list.add(pageId);
				}
			}
			return list.iterator();
		});
		return startPageRDD.count();
	}

	/**
	 * 计算页面切片转换率
	 * @param taskParams
	 * @param pageSplitPvMap
	 * @param startPagePv
	 * @return
	 */
	private static Map<String, Double> computePageSplitConvertRate(
			JSONObject taskParams,
			Map<String, Long> pageSplitPvMap,
			long startPagePv) {

		String targetPageFlow = ParamUtils.getParam(taskParams, Constants.PARAM_TARGET_PAGE_FLOW);
		String[] targetPages = targetPageFlow.split(",");
		long lastPageSplitPv = 0L;
		Map<String, Double> convertRateMap = new HashMap<String, Double>();
		for (int i = 1; i < targetPages.length; i++) {
			String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
			long targetPageSplitPv = pageSplitPvMap.get(targetPageSplit);
			double convertRate = 0.0;
			if (i == 1) {
				convertRate = NumberUtils.formatDouble(
						(double)targetPageSplitPv / (double)startPagePv, 2);
			}else {
				convertRate = NumberUtils.formatDouble(
						(double)targetPageSplitPv / (double)lastPageSplitPv, 2);
			}
			convertRateMap.put(targetPageSplit, convertRate);
			lastPageSplitPv = targetPageSplitPv;
		}
		return convertRateMap;
	}

	/**
	 * 持久化页面切片的转换率
	 * @param taskid
	 * @param convertRateMap
	 */
	private static void persistConvertRate(
			long taskid,
			Map<String, Double> convertRateMap){

		PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
		StringBuffer buffer = new StringBuffer("");
		for (Map.Entry<String, Double> entry : convertRateMap.entrySet()) {
			String pageSplit = entry.getKey();
			Double convertRate = entry.getValue();
			buffer.append(pageSplit + "=" + convertRate + "|");
		}

		String convertRate = buffer.toString();
		convertRate = convertRate.substring(0, convertRate.length() - 1);

		pageSplitConvertRate.setTaskid(taskid);
		pageSplitConvertRate.setConvertRate(convertRate);
		IPageSplitConvertRateDAO pageSplitConvertRateImpl = DAOFactory.getPageSplitConvertRateDAOImpl();
		pageSplitConvertRateImpl.insert(pageSplitConvertRate);
	}


}
