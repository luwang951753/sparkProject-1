package com.ish.sparkproject.spark.session;

import com.ish.sparkproject.constant.Constants;
import com.ish.sparkproject.utils.StringUtils;
import org.apache.spark.util.AccumulatorV2;

/**
 * session聚合统计Accumulator
 */
public class SessionAggrStatAccumulator extends AccumulatorV2<String, String> {

	String accumulatorVal = Constants.SESSION_COUNT + "=0" + "|"
			+ Constants.TIME_PERIOD_1s_3s + "=0" + "|"
			+ Constants.TIME_PERIOD_4s_6s + "=0" + "|"
			+ Constants.TIME_PERIOD_7s_9s + "=0" + "|"
			+ Constants.TIME_PERIOD_10s_30s + "=0" + "|"
			+ Constants.TIME_PERIOD_30s_60s + "=0" + "|"
			+ Constants.TIME_PERIOD_1m_3m + "=0" + "|"
			+ Constants.TIME_PERIOD_3m_10m + "=0" + "|"
			+ Constants.TIME_PERIOD_10m_30m + "=0" + "|"
			+ Constants.TIME_PERIOD_30m + "=0" + "|"
			+ Constants.STEP_PERIOD_1_3 + "=0" + "|"
			+ Constants.STEP_PERIOD_4_6 + "=0" + "|"
			+ Constants.STEP_PERIOD_7_9 + "=0" + "|"
			+ Constants.STEP_PERIOD_10_30 + "=0" + "|"
			+ Constants.STEP_PERIOD_30_60 + "=0" + "|"
			+ Constants.STEP_PERIOD_60 + "=0";

	/**
	 * 对应区间范围计数
	 * @param v 取值:区间范围1s_3s,4s_6s,7s_9s,...,30_60,60
	 */
	@Override
	public void add(String v) {
		String oldVal = StringUtils.getFieldFromConcatString(this.accumulatorVal, "\\|", v);
		if (oldVal != null){
			int newVal = Integer.valueOf(oldVal) + 1;
			this.accumulatorVal = StringUtils.setFieldInConcatString(
					this.accumulatorVal, "\\|", v, String.valueOf(newVal));
		}

	}

	@Override
	public boolean isZero() {
		return	accumulatorVal == Constants.SESSION_COUNT + "=0" + "|"
				+ Constants.TIME_PERIOD_1s_3s + "=0" + "|"
				+ Constants.TIME_PERIOD_4s_6s + "=0" + "|"
				+ Constants.TIME_PERIOD_7s_9s + "=0" + "|"
				+ Constants.TIME_PERIOD_10s_30s + "=0" + "|"
				+ Constants.TIME_PERIOD_30s_60s + "=0" + "|"
				+ Constants.TIME_PERIOD_1m_3m + "=0" + "|"
				+ Constants.TIME_PERIOD_3m_10m + "=0" + "|"
				+ Constants.TIME_PERIOD_10m_30m + "=0" + "|"
				+ Constants.TIME_PERIOD_30m + "=0" + "|"
				+ Constants.STEP_PERIOD_1_3 + "=0" + "|"
				+ Constants.STEP_PERIOD_4_6 + "=0" + "|"
				+ Constants.STEP_PERIOD_7_9 + "=0" + "|"
				+ Constants.STEP_PERIOD_10_30 + "=0" + "|"
				+ Constants.STEP_PERIOD_30_60 + "=0" + "|"
				+ Constants.STEP_PERIOD_60 + "=0";
	}

	@Override
	public AccumulatorV2 copy() {
		SessionAggrStatAccumulator newAccumulator = new SessionAggrStatAccumulator();
		newAccumulator.accumulatorVal = this.accumulatorVal;
		return newAccumulator;
	}

	@Override
	public void reset() {
		accumulatorVal = Constants.SESSION_COUNT + "=0" + "|"
				+ Constants.TIME_PERIOD_1s_3s + "=0" + "|"
				+ Constants.TIME_PERIOD_4s_6s + "=0" + "|"
				+ Constants.TIME_PERIOD_7s_9s + "=0" + "|"
				+ Constants.TIME_PERIOD_10s_30s + "=0" + "|"
				+ Constants.TIME_PERIOD_30s_60s + "=0" + "|"
				+ Constants.TIME_PERIOD_1m_3m + "=0" + "|"
				+ Constants.TIME_PERIOD_3m_10m + "=0" + "|"
				+ Constants.TIME_PERIOD_10m_30m + "=0" + "|"
				+ Constants.TIME_PERIOD_30m + "=0" + "|"
				+ Constants.STEP_PERIOD_1_3 + "=0" + "|"
				+ Constants.STEP_PERIOD_4_6 + "=0" + "|"
				+ Constants.STEP_PERIOD_7_9 + "=0" + "|"
				+ Constants.STEP_PERIOD_10_30 + "=0" + "|"
				+ Constants.STEP_PERIOD_30_60 + "=0" + "|"
				+ Constants.STEP_PERIOD_60 + "=0";
	}

	@Override
	public void merge(AccumulatorV2 other) {
		SessionAggrStatAccumulator otherAcc= (SessionAggrStatAccumulator)other;
		String[] accWords = accumulatorVal.split("[|]");
		String[] oAccWords = otherAcc.accumulatorVal.split("[|]");
		for (int i = 0; i < accWords.length; i++) {
			for (int j = 0; j < oAccWords.length; j++) {
				if (accWords[i].split("[=]")[0].equals(oAccWords[j].split("[=]")[0])){
					int val = Integer.valueOf(accWords[i].split("[=]")[1])
							+Integer.valueOf(oAccWords[j].split("[=]")[1]);
					String partitionVal = StringUtils.setFieldInConcatString(
							this.accumulatorVal,
							"\\|",
							oAccWords[j].split("[=]")[0],
							String.valueOf(val));
					this.accumulatorVal = partitionVal;
				}
			}
		}
	}

	@Override
	public String value() {
		return accumulatorVal;
	}
}
