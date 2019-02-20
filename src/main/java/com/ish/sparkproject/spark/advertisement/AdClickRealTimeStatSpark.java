package com.ish.sparkproject.spark.advertisement;

import com.ish.sparkproject.conf.ConfigurationManager;
import com.ish.sparkproject.constant.Constants;
import com.ish.sparkproject.dao.IAdUserClickCountDAO;
import com.ish.sparkproject.dao.factory.DAOFactory;
import com.ish.sparkproject.domain.AdBlacklist;
import com.ish.sparkproject.domain.AdStat;
import com.ish.sparkproject.domain.AdUserClickCount;
import com.ish.sparkproject.utils.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;


/**
 * 实时广告推送模块
 */
public class AdClickRealTimeStatSpark {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_AD)
				.setMaster("local[2]");
		JavaStreamingContext jssc = new JavaStreamingContext(
				conf, Durations.seconds(5));

		/*
		* 正式开始业务开发
		* 1. 针对Kafka数据来源的输入DStream
		* 2. 选用Kafka的direct API(优点: 内部自适应调整每次接受数据量的特性)
		* 3. 获取从kafka集群里的topics中获取JavaPairInputDStream
		* */

		// 构建Kafka的参数Map<String, String>,用来存放Kafka集群的连接地址.
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST,
				ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));

		// 构建Kafka的topic set
		String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
		String[] kafkaTopicSplited = kafkaTopics.split(",");
		Set<String> topics = new HashSet<String>();
		for (String kafkaTopic : kafkaTopicSplited) {
			topics.add(kafkaTopic);
		}

		// 基于kafka direct模式构建出针对kafka集群中指定topic的JavaPairInputDStream
		JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
				jssc,
				String.class, // 键key类型
				String.class, // 值value类型
				StringDecoder.class, // 键key的解码类型
				StringDecoder.class, // 值value的解码类型
				kafkaParams, // kafka集群的通信地址
				topics);// kafka的topics

		// 原始数据根据黑名单进行过滤
		JavaPairDStream<String, String> filteredAdRealTimeLogDStream =
				filterByBlacklist(adRealTimeLogDStream);

		// 生成动态黑名单
		genDynamicBlacklist(filteredAdRealTimeLogDStream);

		// 业务功能一: 每天各省市用户对应广告点击量的实时统计
		JavaPairDStream<String, Long> adRealTimeStatDStream =
				calculateRealTimeStat(filteredAdRealTimeLogDStream);

		// 业务功能二: 每天各省市top3热门广告


		// 业务功能三: 每天每个广告在最近一小时滑动窗口内的点击趋势(每分钟的点击量)



		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		jssc.close();

	}
// =======================================================================
	/**
	 * 根据动态黑名单对原始数据进行过滤
	 * @param adRealTimeLogDStream
	 * @return
	 */
	private static JavaPairDStream<String, String> filterByBlacklist(
			JavaPairDStream<String, String> adRealTimeLogDStream){

		JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(rdd -> {
			List<AdBlacklist> adBlacklists = DAOFactory.getAdBlacklistDAOImpl().findAll();

			// 对ad_blacklist数据表查询,并将结果封装为Tuple2<Long, Boolean>(userid, true)
			List<Tuple2<Long, Boolean>> tuples = new ArrayList<>();
			for (AdBlacklist adBlacklist : adBlacklists) {
				tuples.add(new Tuple2<Long, Boolean>(adBlacklist.getUserid(), true));
			}
			JavaSparkContext jsc = new JavaSparkContext(rdd.context());
			JavaPairRDD<Long, Boolean> blacklistRDD = jsc.parallelizePairs(tuples);

			// 将原始数据进行map转换为Tuple2<Long, Tuple2<String, String>>(userid, tuple)
			JavaPairRDD<Long, Tuple2<String, String>> mappedRDD =
					rdd.mapToPair(tuple -> {
						String log = tuple._2;
						String[] logsplited = log.split(" ");
						long userid = Long.valueOf(logsplited[3]);
						return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);
					});

			// 将map转换后的数据与ad_blacklist的数据进行左连接的join,并对结果存在于ad_blacklist的数据进行过滤
			JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD =
					mappedRDD.leftOuterJoin(blacklistRDD);

			JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD =
					joinedRDD.filter(tuple -> {
						Optional<Boolean> optional = tuple._2._2;
						if (optional.isPresent() && optional.get()) {
							return false;
						}
						return true;
					});

			// 获取最终不在黑名单中的数据
			JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(tuple -> {
				return tuple._2._1;
			});
			return resultRDD;
		});
		return filteredAdRealTimeLogDStream;
	}

	/**
	 * 生成动态黑名单
	 * @param filteredAdRealTimeLogDStream
	 */
	private static void genDynamicBlacklist(
			JavaPairDStream<String, String> filteredAdRealTimeLogDStream){

		// 已经获取格式为<timestamp province city userid adid>的一条条实时的数据
		// 计算出每隔5秒内的数据流中,每天每个用户每个广告的点击量
		JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream.mapToPair(tuple -> {
			// 获取数据流中的日志数据,并按照空格切割
			String log = tuple._2;
			String[] logsplited = log.split(" ");

			// 获取时间戳并转换为String类型
			String timestamp = logsplited[0];
			Date date = new Date(Long.valueOf(timestamp));
			String dateKey = DateUtils.formatDateKey(date);

			String userid = logsplited[3];
			String adid = logsplited[4];

			// 拼接key
			String key = dateKey + "_" + userid + "_" + adid;

			return new Tuple2<String, Long>(key, 1L);
		});

		// 针对处理后的日志格式,执行reduceByKey()算子即可
		// (每个batch中)每天每个用户对广告的点击量,数据格式<yyyyMMdd_01_21, 1265>
		JavaPairDStream<String, Long> dailyUserAdClickCountDStream =
				dailyUserAdClickDStream.reduceByKey((v1, v2) -> v1 + v2);

		// 获取的数据格式<yyyyMMdd_userid_adid, count>
		// 切分出(yyyyMMdd, userid, adid, clickCount)并将结果写入MySQL

		dailyUserAdClickCountDStream.foreachRDD(rdd -> {
			rdd.foreachPartition(iterator -> {
				List<AdUserClickCount> adUserClickCounts = new ArrayList<>();
				while (iterator.hasNext()){
					Tuple2<String, Long> tuple = iterator.next();
					String[] splited = tuple._1.split("_");
					AdUserClickCount adUserClickCount = new AdUserClickCount();
					// 将 yyyyMMdd 格式转化为 yyyy-MM-dd
					String date = DateUtils.formatDate(DateUtils.parseDateKey(splited[0]));
					long userid = Long.valueOf(splited[1]);
					long adid = Long.valueOf(splited[2]);
					Long clickCount = tuple._2;

					adUserClickCount.setDate(date);
					adUserClickCount.setUserid(userid);
					adUserClickCount.setAdid(adid);
					adUserClickCount.setClickCount(clickCount);
					adUserClickCounts.add(adUserClickCount);
				}
				IAdUserClickCountDAO adUserClickCountDAOImpl =
						DAOFactory.getAdUserClickCountDAOImpl();
				adUserClickCountDAOImpl.updateBatch(adUserClickCounts);
			});
		});

		//	现在我们在mysql里面，已经有了累计的每天各用户对各广告的点击量
		//	遍历每个batch中的所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
		//	从mysql中查询
		//	查询出来的结果，如果是100，如果你发现某个用户某天对某个广告的点击量已经大于等于100了
		//	那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化
		//
		//	对batch中的数据，去查询mysql中的点击次数，使用哪个DStream呢？
		//	dailyUserAdClickCountDStream
		//	为什么用这个batch？因为这个batch是聚合过的数据，已经按照yyyyMMdd_userid_adid进行过聚合了
		//	比如原始数据可能是一个batch有一万条，聚合过后可能只有五千条
		//	所以选用这个聚合后的DStream，既可以满足咱们的需求，而且呢，还可以尽量减少要处理的数据量
		//	一石二鸟，一举两得

		JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(tuple -> {
			String[] keySplited = tuple._1.split("_");
			// 获取日期, 并格式转换: yyyyMMdd -> yyyy-MM-dd
			String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
			// 获取 userid, adid, clickCount
			long userid = Long.valueOf(keySplited[1]);
			long adid = Long.valueOf(keySplited[2]);

			int clickCount = DAOFactory.getAdUserClickCountDAOImpl().findClickCountByMultiKey(
					date, userid, adid);

			if (clickCount >= 100) {
				return true;
			}
			return false;
		});

		// blacklistDStream
		// 里面的每个batch，其实就是都是过滤出来的已经在某天对某个广告点击量超过100的用户
		// 遍历这个dstream中的每个rdd，然后将黑名单用户增加到mysql中
		// 这里一旦增加以后，在整个这段程序的前面，会加上根据黑名单动态过滤用户的逻辑
		// 我们可以认为，一旦用户被拉入黑名单之后，以后就不会再出现在这里了
		// 所以直接插入mysql即可

		// 我们有没有发现这里有一个小小的问题？
		// blacklistDStream中，可能有userid是重复的，如果直接这样插入的话
		// 那么是不是会发生，插入重复的黑明单用户
		// 我们在插入前要进行去重
		// yyyyMMdd_userid_adid
		// 20151220_10001_10002 100
		// 20151220_10001_10003 100
		// 10001这个userid就重复了

		/*
		这段代码,去重复userid是有问题的,因为rdd.foreachPartition这个方法只会对rdd的分区执行去重,
		但是DStream中不同批次的rdd里面还是会有重复,所以要对DStream进行去重才能真正实现没有重复的userid!

		blacklistDStream.foreachRDD(rdd -> {
			rdd.foreachPartition(iterator -> {
				List<AdBlacklist> adBlacklists = new ArrayList<>();
				List<Long> userids = new ArrayList<>();
				if (iterator.hasNext()){
					Tuple2<String, Long> tuple = iterator.next();
					String[] splitedKey = tuple._1.split("_");
					long userid = Long.valueOf(splitedKey[1]);
					if (!userids.contains(userid)){
						userids.add(userid);
						AdBlacklist adBlacklist = new AdBlacklist();
						adBlacklist.setUserid(userid);
						adBlacklists.add(adBlacklist);
					}
				}
			});
		});
		 */

		JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(tuple -> {
			String[] keySplited = tuple._1.split("_");
			Long userid = Long.valueOf(keySplited[1]);
			return userid;
		});

		// 使用transform()方法操作DS中的RDD.distinct()进行去重
		JavaDStream<Long> distinctedBlacklistUseridDStream =
				blacklistUseridDStream.transform(rdd -> {
					return rdd.distinct();
				});

		// 黑名单的数据结构写入MySQL
		distinctedBlacklistUseridDStream.foreachRDD(rdd -> {
			rdd.foreachPartition(iterator -> {
				List<AdBlacklist> adBlacklists = new ArrayList<>();
				while (iterator.hasNext()){
					Long userid = iterator.next();
					AdBlacklist adBlacklist = new AdBlacklist();
					adBlacklist.setUserid(userid);
					adBlacklists.add(adBlacklist);
				}
				DAOFactory.getAdBlacklistDAOImpl().insertBatch(adBlacklists);
			});
		});
	}

	/**
	 * 各省市用户对应广告点击量的实时统计
	 * @param filteredAdRealTimeLogDStream
	 * @return
	 */
	private static JavaPairDStream<String, Long> calculateRealTimeStat(
			JavaPairDStream<String, String> filteredAdRealTimeLogDStream){
		// 获取黑名单过滤后的数据,并设计新的key值
		JavaPairDStream<String, Long> mappedDStream = filteredAdRealTimeLogDStream.mapToPair(tuple -> {
			String[] logSplited = tuple._2.split(" ");
			String timestamp = logSplited[0];
			Date date = new Date(Long.valueOf(timestamp));
			String dateKey = DateUtils.formatDateKey(date);
			String province = logSplited[1];
			String city = logSplited[2];
			long adid = Long.valueOf(logSplited[4]);

			String key = dateKey + "_" + province + "_" + city + "_" + adid;
			return new Tuple2<String, Long>(key, 1L);
		});

		// updateStateByKey同步更新不同batch中相同key的value数据
		JavaPairDStream<String, Long> aggregatedDStream =
				mappedDStream.updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
					@Override
					public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
						long clickCount = 0;
						if (optional.isPresent()) {
							clickCount = optional.get();
						}
						for (long value : values) {
							clickCount += value;
						}
						return Optional.of(clickCount);
					}
				});

		// 将动态聚合后的数据持久化到MySQL
		aggregatedDStream.foreachRDD(rdd -> {
			rdd.foreachPartition(iterator -> {
				List<AdStat> adStats = new ArrayList<>();
				while (iterator.hasNext()){
					Tuple2<String, Long> tuple = iterator.next();
					String[] keySplited = tuple._1.split("_");
					String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
					String province = keySplited[1];
					String city = keySplited[2];
					long adid = Long.valueOf(keySplited[3]);
					long clickCount = tuple._2;

					AdStat adStat = new AdStat();
					adStat.setDate(date);
					adStat.setProvince(province);
					adStat.setCity(city);
					adStat.setAdid(adid);
					adStat.setClickCount(clickCount);

					adStats.add(adStat);
				}
				DAOFactory.getAdStatDAPImpl().updateBatch(adStats);
			});
		});
		return aggregatedDStream;
	}
}
