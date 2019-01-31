package com.ish.sparkproject.dao.impl;

import com.ish.sparkproject.dao.ITaskDAO;
import com.ish.sparkproject.dao.factory.DAOFactory;
import com.ish.sparkproject.domain.Task;
import org.junit.Test;

public class TaskDAOImplTest {

	@Test
	public void findById() {
		ITaskDAO taskDAOImpl = DAOFactory.getTaskDAOImpl();
		Task task = taskDAOImpl.findById(1);
		System.out.println(task.toString());
	}
}