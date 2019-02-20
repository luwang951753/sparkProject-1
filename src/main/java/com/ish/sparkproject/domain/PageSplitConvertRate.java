package com.ish.sparkproject.domain;

public class PageSplitConvertRate {
	private long taskid;
	private String convertRate;

	public PageSplitConvertRate() {
	}

	public PageSplitConvertRate(long taskid, String convertRate) {
		this.taskid = taskid;
		this.convertRate = convertRate;
	}

	public long getTaskid() {
		return taskid;
	}

	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}

	public String getConvertRate() {
		return convertRate;
	}

	public void setConvertRate(String convertRate) {
		this.convertRate = convertRate;
	}
}
