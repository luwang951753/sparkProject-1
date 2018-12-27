package com.ish.sparkproject.conf;

import org.junit.Test;

public class ConfigurationManagerTest {

	@Test
	public void testGetProperty() {
		String v1 = ConfigurationManager.getProperty("testkey1");
		String v2 = ConfigurationManager.getProperty("testkey2");
		System.out.println(v1);
		System.out.println(v2);
	}
}