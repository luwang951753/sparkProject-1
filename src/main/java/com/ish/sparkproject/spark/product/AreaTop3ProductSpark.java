package com.ish.sparkproject.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.ish.sparkproject.conf.ConfigurationManager;
import com.ish.sparkproject.constant.Constants;
import com.ish.sparkproject.dao.ITaskDAO;
import com.ish.sparkproject.dao.factory.DAOFactory;
import com.ish.sparkproject.domain.AreaTop3Product;
import com.ish.sparkproject.domain.Task;
import com.ish.sparkproject.utils.ParamUtils;
import com.ish.sparkproject.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 各个区域热门商品统计spark作业
 */
public class AreaTop3ProductSpark {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_PRODUCT)
				.setMaster("local[2]");
		SparkSession sparkSession = SparkSession.builder()
				.config(conf)
				.getOrCreate();

		// 模拟数据
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
		String startDate = ParamUtils.getParam(taskParams, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParams, Constants.PARAM_END_DATE);

		// 注册自定义UDF和UDAF函数
		sparkSession.udf().register(
				"concat_Long_String",
				new ConcatLongStringUDF(),
				DataTypes.StringType);
		sparkSession.udf().register(
				"group_concat_distinct",
				new GroupConcatDistinctUDAF());
		sparkSession.udf().register(
				"get_json_object",
				new GetJsonObjectUDF(),
				DataTypes.StringType);
		sparkSession.udf().register(
				"random_prefix",
				new RandomPrefixUDF(),
				DataTypes.StringType);
		sparkSession.udf().register(
				"remove_random_prefix",
				new RemoveRandomPrefixUDF(),
				DataTypes.StringType);

		// 模拟数据
		SparkUtils.mockData(sparkSession);

		JavaPairRDD<Long, Row> cityId2ClickActionRDD = getCityid2ClickActionRDDByDate(sparkSession, startDate, endDate);
		JavaPairRDD<Long, Row> cityId2cityInfoRDD = getCityid2CityInfo(sparkSession);

		generateClickProductBasicTable(sparkSession, cityId2ClickActionRDD, cityId2cityInfoRDD);
		genTempAreaProductClickCountTable(sparkSession);
		genTempAreaFullProductClickCountTable(sparkSession);

		JavaRDD<Row> areaTop3ProductRDD = getAreaTop3Product(sparkSession);

		// 因为区域Top3热门商品的数据量很小,所以可以collect到本地,然后选择使用批量插入数据到MySQL数据库
		List<Row> areaTop3ProductList = areaTop3ProductRDD.collect();

		persistAreaTop3Product(taskid, areaTop3ProductList);

		sparkSession.close();
	}

	/**
	 * 查询指定日期范围内的点击行为数据
	 * @param sparkSession
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	private static JavaPairRDD<Long, Row> getCityid2ClickActionRDDByDate(
			SparkSession sparkSession,
			String startDate,
			String endDate){

		String sql =
				"select " +
					"city_id, " +
					"click_product_id, " +
					"product_id " +
				"from user_visit_action " +
					"where click_product_id is not null " +
					"and click_product_id != 'NULL' " +
					"and click_product_id != 'null' " +
					"and date >= '" + startDate +"' " +
					"and date <= '" + endDate + "'";

		Dataset<Row> clickActionDS = sparkSession.sql(sql);
		JavaPairRDD<Long, Row> cityid2ClickActionRDD = clickActionDS.javaRDD().mapToPair(row -> {
			long cityId = row.getLong(0);
			return new Tuple2<Long, Row>(cityId, row);
		});
		return cityid2ClickActionRDD;
	}

	/**
	 * 从MySQL中查询城市信息
	 * @param sparkSession
	 * @return
	 */
	private static JavaPairRDD<Long, Row> getCityid2CityInfo(SparkSession sparkSession){
		// 构建MySQL的连接信息
		String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
		String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
		String passwd = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
		Map<String, String> options = new HashMap<>();
		options.put("url", url);
		options.put("dbTable", "city_info");
		options.put("user", user);
		options.put("password", passwd);

		// 获取MySQL的对应表数据
		Dataset<Row> cityInfoDS = sparkSession.read().format("jdbc").options(options).load();
		JavaPairRDD<Long, Row> cityId2CityInfoRDD = cityInfoDS.javaRDD().mapToPair(row -> {
			long cityId = row.getLong(0);
			return new Tuple2<>(cityId, row);
		});
		return cityId2CityInfoRDD;
	}

	/**
	 * 生成点击商品基础信息临时表
	 * @param sparkSession
	 * @param cityId2ClickActionRDD
	 * @param cityId2cityInfoRDD
	 * @return
	 */
	private static void generateClickProductBasicTable(
			SparkSession sparkSession,
			JavaPairRDD<Long, Row> cityId2ClickActionRDD,
			JavaPairRDD<Long, Row> cityId2cityInfoRDD){

		JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = cityId2ClickActionRDD.join(cityId2cityInfoRDD);
		JavaRDD<Row> mapedRDD = joinedRDD.map(tuple -> {
			long cityId = tuple._1;
			Row clickActionRow = tuple._2._1;
			Row cityInfoRow = tuple._2._2;
			long productId = clickActionRow.getLong(1);
			String cityName = cityInfoRow.getString(1);
			String area = cityInfoRow.getString(2);
			return RowFactory.create(cityId, cityName, area, productId);
		});

		// 基于JavaRDD<Row>转换为Dataset<Row>
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
		structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));
		StructType schema = DataTypes.createStructType(structFields);

		Dataset<Row> dataFrame = sparkSession.createDataFrame(mapedRDD, schema);

		// 将dataFrame中的数据注册成临时表(tmp_clk_prod_basic)
		dataFrame.createOrReplaceTempView("tmp_clk_prod_basic");
	}

	/**
	 * 生成各区域商品点击数临时表
	 * @param sparkSession
	 */
	private static void genTempAreaProductClickCountTable(SparkSession sparkSession){
		String sql =
				"SELECT " +
					"product_id_area," +
					"count(click_count) click_count," +
					"group_concat_distinct(city_infos) city_infos " +
				"FROM (" +
						"SELECT" +
							"remove_random_prefix(product_id_area) real_key," +
							"count(*) click_count," +
							"city_infos " +
						"FROM (" +
							"SELECT" +
								"product_id_area," +
								"count(*) click_count," +
								"group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos " +
							"FROM (" +
								"SELECT " +
									"random_prefix(area, product_id, ':') product_id_area," +
									"city_id," +
									"city_name " +
								"FROM " +
									"tmp_clk_prod_basic" +
							") t1 " +
							"GROUP BY product_id_area" +
						") t2" +
				") t3 " +
				"GROUP BY real_key";

		Dataset<Row> df = sparkSession.sql(sql);
		df.createOrReplaceTempView("tmp_area_product_click_count");

	}

	/**
	 * 生成各区域商品点击次数完整信息的临时表
	 * @param sparkSession
	 */
	private static void genTempAreaFullProductClickCountTable(SparkSession sparkSession) {
		String sql = "select " +
				"tapcc.area, " +
				"tapcc.product_id, " +
				"tapcc.click_count, " +
				"tapcc.city_infos, " +
				"pi.product_name, " +
				"if(get_json_object(extend_info, 'product_status')=0, '自营', '第三方') product_status" +
				"from tmp_area_product_click_count tapcc " +
				"join product_info pi " +
				"where tapcc.product_id = pi.product_id;";
		Dataset<Row> df = sparkSession.sql(sql);
		df.createOrReplaceTempView("tmp_area_fullprod_click_count");
	}

	/**
	 * 获取各区域Top3热门商品
	 * @param sparkSession
	 * @return
	 */
	private static JavaRDD<Row> getAreaTop3Product(SparkSession sparkSession){
		String sql =
				"SELECT " +
					"area," +
					"CASE " +
						"WHEN area = '华北' OR area = '华东' THEN 'A级'" +
						"WHEN area = '华南' OR area = '华中' THEN 'B级'" +
						"WHEN area = '西北' OR area = '西南' THEN 'C级'" +
						"ELSE 'D级'" +
					"END area_level" +
					"product_id, " +
					"click_count, " +
					"city_infos, " +
					"product_name, " +
					"product_status, " +
				"FROM (" +
					"SELECT " +
						"area, " +
						"product_id, " +
						"click_count, " +
						"city_infos, " +
						"product_name, " +
						"product_status," +
						"ROW_NUMBER() OVER (PARTITION BY area ORDER BY click_count DESC) rank" +
					"FROM " +
						"tmp_area_fullprod_click_count) t" +
				"WHERE t.rank <= 3;";
		Dataset<Row> df = sparkSession.sql(sql);
		return df.javaRDD();
	}

	/**
	 * 将各个区域的Top3热门商品写入Mysql
	 * @param rows
	 */
	private static void persistAreaTop3Product(long taskid, List<Row> rows) {
		List<AreaTop3Product> areaTop3Products = new ArrayList<>();
		for (Row row : rows) {
			AreaTop3Product areaTop3Product = new AreaTop3Product();
			areaTop3Product.setTaskid(taskid);
			areaTop3Product.setArea(row.getString(0));
			areaTop3Product.setAreaLevel(row.getString(1));
			areaTop3Product.setProductid(row.getLong(2));
			areaTop3Product.setCityInfos(row.getString(4));
			areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));
			areaTop3Product.setProductName(row.getString(5));
			areaTop3Product.setProductStatus(row.getString(6));
			areaTop3Products.add(areaTop3Product);
		}
		DAOFactory.getAreaTop3ProductDAOImpl().insertBatch(areaTop3Products);
	}
}
