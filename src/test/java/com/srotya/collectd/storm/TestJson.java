package com.srotya.collectd.storm;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * Unit test for simple App.
 */
public class TestJson {

	@Test
	public void testGsonNull() {
		String json = "{ \"test\": null }";
		Type type = new TypeToken<HashMap<String, Object>>() {
		}.getType();
		Gson gson = new Gson();
		Map<String, Object> fromJson = gson.fromJson(json, type);
		System.out.println(""+fromJson.get("test"));
	}

}
