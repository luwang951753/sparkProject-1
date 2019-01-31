package com.ish.sparkproject.domain;

/**
 * session_aggr_stat表对象
 */
public class SessionAggrStat {
	private long taskid;
	private long session_count;
	private double visitLength_1s_3s;
	private double visitLength_4s_6s;
	private double visitLength_7s_9s;
	private double visitLength_10s_30s;
	private double visitLength_30s_60s;
	private double visitLength_1m_3m;
	private double visitLength_3m_10m;
	private double visitLength_10m_30m;
	private double visitLength_30m;
	private double stepLength_1_3;
	private double stepLength_4_6;
	private double stepLength_7_9;
	private double stepLength_10_30;
	private double stepLength_30_60;
	private double stepLength_60;

	public long getTaskid() {
		return taskid;
	}

	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}

	public long getSession_count() {
		return session_count;
	}

	public void setSession_count(long session_count) {
		this.session_count = session_count;
	}

	public double getVisitLength_1s_3s() {
		return visitLength_1s_3s;
	}

	public void setVisitLength_1s_3s(double visitLength_1s_3s) {
		this.visitLength_1s_3s = visitLength_1s_3s;
	}

	public double getVisitLength_4s_6s() {
		return visitLength_4s_6s;
	}

	public void setVisitLength_4s_6s(double visitLength_4s_6s) {
		this.visitLength_4s_6s = visitLength_4s_6s;
	}

	public double getVisitLength_7s_9s() {
		return visitLength_7s_9s;
	}

	public void setVisitLength_7s_9s(double visitLength_7s_9s) {
		this.visitLength_7s_9s = visitLength_7s_9s;
	}

	public double getVisitLength_10s_30s() {
		return visitLength_10s_30s;
	}

	public void setVisitLength_10s_30s(double visitLength_10s_30s) {
		this.visitLength_10s_30s = visitLength_10s_30s;
	}

	public double getVisitLength_30s_60s() {
		return visitLength_30s_60s;
	}

	public void setVisitLength_30s_60s(double visitLength_30s_60s) {
		this.visitLength_30s_60s = visitLength_30s_60s;
	}

	public double getVisitLength_1m_3m() {
		return visitLength_1m_3m;
	}

	public void setVisitLength_1m_3m(double visitLength_1m_3m) {
		this.visitLength_1m_3m = visitLength_1m_3m;
	}

	public double getVisitLength_3m_10m() {
		return visitLength_3m_10m;
	}

	public void setVisitLength_3m_10m(double visitLength_3m_10m) {
		this.visitLength_3m_10m = visitLength_3m_10m;
	}

	public double getVisitLength_10m_30m() {
		return visitLength_10m_30m;
	}

	public void setVisitLength_10m_30m(double visitLength_10m_30m) {
		this.visitLength_10m_30m = visitLength_10m_30m;
	}

	public double getVisitLength_30m() {
		return visitLength_30m;
	}

	public void setVisitLength_30m(double visitLength_30m) {
		this.visitLength_30m = visitLength_30m;
	}

	public double getStepLength_1_3() {
		return stepLength_1_3;
	}

	public void setStepLength_1_3(double stepLength_1_3) {
		this.stepLength_1_3 = stepLength_1_3;
	}

	public double getStepLength_4_6() {
		return stepLength_4_6;
	}

	public void setStepLength_4_6(double stepLength_4_6) {
		this.stepLength_4_6 = stepLength_4_6;
	}

	public double getStepLength_7_9() {
		return stepLength_7_9;
	}

	public void setStepLength_7_9(double stepLength_7_9) {
		this.stepLength_7_9 = stepLength_7_9;
	}

	public double getStepLength_10_30() {
		return stepLength_10_30;
	}

	public void setStepLength_10_30(double stepLength_10_30) {
		this.stepLength_10_30 = stepLength_10_30;
	}

	public double getStepLength_30_60() {
		return stepLength_30_60;
	}

	public void setStepLength_30_60(double stepLength_30_60) {
		this.stepLength_30_60 = stepLength_30_60;
	}

	public double getStepLength_60() {
		return stepLength_60;
	}

	public void setStepLength_60(double stepLength_60) {
		this.stepLength_60 = stepLength_60;
	}

	public SessionAggrStat() {

	}

	public SessionAggrStat(
			long taskid,
			long session_count,
			double visitLength_1s_3s,
			double visitLength_4s_6s,
			double visitLength_7s_9s,
			double visitLength_10s_30s,
			double visitLength_30s_60s,
			double visitLength_1m_3m,
			double visitLength_3m_10m,
			double visitLength_10m_30m,
			double visitLength_30m,
			double stepLength_1_3,
			double stepLength_4_6,
			double stepLength_7_9,
			double stepLength_10_30,
			double stepLength_30_60,
			double stepLength_60) {
		this.taskid = taskid;
		this.session_count = session_count;
		this.visitLength_1s_3s = visitLength_1s_3s;
		this.visitLength_4s_6s = visitLength_4s_6s;
		this.visitLength_7s_9s = visitLength_7s_9s;
		this.visitLength_10s_30s = visitLength_10s_30s;
		this.visitLength_30s_60s = visitLength_30s_60s;
		this.visitLength_1m_3m = visitLength_1m_3m;
		this.visitLength_3m_10m = visitLength_3m_10m;
		this.visitLength_10m_30m = visitLength_10m_30m;
		this.visitLength_30m = visitLength_30m;
		this.stepLength_1_3 = stepLength_1_3;
		this.stepLength_4_6 = stepLength_4_6;
		this.stepLength_7_9 = stepLength_7_9;
		this.stepLength_10_30 = stepLength_10_30;
		this.stepLength_30_60 = stepLength_30_60;
		this.stepLength_60 = stepLength_60;
	}

	@Override
	public String toString() {
		return "SessionAggrStat{" +
				"taskid=" + taskid +
				", session_count=" + session_count +
				", visitLength_1s_3s=" + visitLength_1s_3s +
				", visitLength_4s_6s=" + visitLength_4s_6s +
				", visitLength_7s_9s=" + visitLength_7s_9s +
				", visitLength_10s_30s=" + visitLength_10s_30s +
				", visitLength_30s_60s=" + visitLength_30s_60s +
				", visitLength_1m_3m=" + visitLength_1m_3m +
				", visitLength_3m_10m=" + visitLength_3m_10m +
				", visitLength_10m_30m=" + visitLength_10m_30m +
				", visitLength_30m=" + visitLength_30m +
				", stepLength_1_3=" + stepLength_1_3 +
				", stepLength_4_6=" + stepLength_4_6 +
				", stepLength_7_9=" + stepLength_7_9 +
				", stepLength_10_30=" + stepLength_10_30 +
				", stepLength_30_60=" + stepLength_30_60 +
				", stepLength_60=" + stepLength_60 +
				'}';
	}
}
