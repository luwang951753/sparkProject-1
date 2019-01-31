package com.ish.sparkproject.dao;

import com.ish.sparkproject.domain.Task;

public interface ITaskDAO {
	/**
	 * 根据taskid找到对应的任务
	 * @return Task
	 */
	public Task findById(long taskid);
}
