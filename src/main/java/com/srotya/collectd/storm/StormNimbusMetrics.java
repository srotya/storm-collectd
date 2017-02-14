/**
 * Copyright 2017 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.collectd.storm;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.collectd.api.Collectd;
import org.collectd.api.CollectdConfigInterface;
import org.collectd.api.CollectdReadInterface;
import org.collectd.api.CollectdShutdownInterface;
import org.collectd.api.OConfigItem;
import org.collectd.api.OConfigValue;
import org.collectd.api.PluginData;
import org.collectd.api.ValueList;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * 
 * 
 * @author ambud
 */
public class StormNimbusMetrics implements CollectdConfigInterface, CollectdReadInterface, CollectdShutdownInterface {

	private static final String SPOUTS = "spouts";
	private static final String BOLTS = "bolts";
	private static final String COMPLETE_LATENCY = "completeLatency";
	private static final String EXECUTE_LATENCY = "executeLatency";
	private static final String PROCESS_LATENCY = "processLatency";
	private static final String FAILED = "failed";
	private static final String ACKED = "acked";
	private static final String CAPACITY = "capacity";
	private static final List<String> BOLT_PROPS = Arrays.asList(FAILED, PROCESS_LATENCY, EXECUTE_LATENCY, ACKED, CAPACITY);
	private static final List<String> SPOUT_PROPS = Arrays.asList(FAILED, COMPLETE_LATENCY, ACKED, CAPACITY);
	private static final String BOLT_ID = "boltId";
	private static final String SPOUT_ID = "spoutId";
	private List<String> nimbusAddresses;
	private boolean kerberos = false;
	private HttpClientBuilder builder;
	private HttpClientContext context;
	private Subject subject;

	public StormNimbusMetrics() {
		Collectd.registerConfig("storm", this);
		Collectd.registerRead("storm", this);
		Collectd.registerShutdown("storm", this);
	}

	@Override
	public int read() {
		Gson gson = new Gson();
		Subject.doAs(subject, new PrivilegedAction<Void>() {

			@Override
			public Void run() {
				for (String nimbus : nimbusAddresses) {
					HttpGet request = new HttpGet(nimbus + "/api/v1/topology/summary");
					CloseableHttpClient client = builder.build();
					try {
						HttpResponse response = client.execute(request, context);
						if (response.getStatusLine().getStatusCode() == 200) {
							HttpEntity entity = response.getEntity();
							String result = EntityUtils.toString(entity);
							JsonObject topologySummary = gson.fromJson(result, JsonObject.class);
							List<String> ids = extractTopologyIds(topologySummary.get("topologies").getAsJsonArray());
							if (ids.isEmpty()) {
								Collectd.logInfo("No storm topologies deployed");
							}
							for (String id : ids) {
								PluginData pd = new PluginData();
								pd.setPluginInstance(id);
								pd.setTime(System.currentTimeMillis());
								try {
									pd.setHost(new URI(nimbus).getHost());
								} catch (URISyntaxException e) {
									continue;
								}
								ValueList values = new ValueList(pd);
								fetchTopologyMetrics(nimbus, id, values, builder, gson);
							}
						} else {
							Collectd.logError("Unable to fetch Storm metrics:" + response.getStatusLine() + "\t"
									+ EntityUtils.toString(response.getEntity()));
						}
						client.close();
					} catch (Exception e) {
						Collectd.logError(
								"Failed to fetch metrics from Nimbus:" + nimbus + "\treason:" + e.getMessage());
						e.printStackTrace();
						continue;
					}
				}
				return null;
			}
		});
		return 0;
	}

	public void fetchTopologyMetrics(String url, String topologyId, ValueList values, HttpClientBuilder builder,
			Gson gson) throws ClientProtocolException, IOException {
		CloseableHttpClient client = builder.build();
		HttpGet get = new HttpGet(url + "/api/v1/topology/" + topologyId + "?window=600");
		CloseableHttpResponse result = client.execute(get, context);
		if (result.getStatusLine().getStatusCode() == 200) {
			String metrics = EntityUtils.toString(result.getEntity());
			JsonObject topologyMetrics = gson.fromJson(metrics, JsonObject.class);
			if (topologyMetrics.has(SPOUTS)) {
				JsonArray spouts = topologyMetrics.get(SPOUTS).getAsJsonArray();
				for (JsonElement spoutElement : spouts) {
					for (String field : SPOUT_PROPS) {
						addDataSourceAndValue(spoutElement, SPOUT_ID, field, values);
					}
				}
			} else {
				Collectd.logError("Topology:" + topologyId + " has no Spouts");
			}

			if (topologyMetrics.has(BOLTS)) {
				JsonArray bolts = topologyMetrics.get(BOLTS).getAsJsonArray();
				for (JsonElement boltElement : bolts) {
					for (String field : BOLT_PROPS) {
						addDataSourceAndValue(boltElement, BOLT_ID, field, values);
					}
				}
			} else {
				Collectd.logError("Topology:" + topologyId + " has no Bolts");
			}
		} else {
			if (result.getStatusLine().getStatusCode() == 401) {
				Collectd.logError("Looks like supplied Kerberos account can't read topology metrics for:" + topologyId);
			} else {
				Collectd.logError("Failed to fetch topology metrics:" + result.getStatusLine() + "\t"
						+ EntityUtils.toString(result.getEntity()));
			}
		}
	}

	public void addDataSourceAndValue(JsonElement element, String idField, String field, ValueList values) {
		JsonObject bolt = element.getAsJsonObject();
		if (field.toLowerCase().contains("latency")) {
			values.setType("latency");
		} else {
			values.setType("records");
		}
		values.setPlugin(bolt.get(idField).getAsString());
		values.setTypeInstance(field);
		values.setValues(Arrays.asList(bolt.get(field).getAsNumber()));
		Collectd.dispatchValues(values);
	}

	public List<String> extractTopologyIds(JsonArray topologies) {
		List<String> topologyIds = new ArrayList<>();
		for (JsonElement topologyElement : topologies) {
			JsonObject topologyObject = topologyElement.getAsJsonObject();
			if (topologyObject.has("id")) {
				String topologyId = topologyObject.get("id").getAsString();
				topologyIds.add(topologyId);
			}
		}
		return topologyIds;
	}

	@Override
	public int config(OConfigItem config) {
		nimbusAddresses = new ArrayList<>();
		String jaasPath = "jaas.conf";
		List<OConfigItem> children = config.getChildren();
		for (OConfigItem child : children) {
			switch (child.getKey().toLowerCase()) {
			case "address":
				for (OConfigValue nimbus : child.getValues()) {
					try {
						new URI(nimbus.toString());
					} catch (Exception e) {
						Collectd.logError("Bad URI " + nimbus + " for Nimbus, error:" + e.getMessage());
						return -1;
					}
					nimbusAddresses.add(nimbus.getString());
				}
				break;
			case "kerberos":
				kerberos = child.getValues().get(0).getBoolean();
				break;
			case "jaas":
				jaasPath = child.getValues().get(0).getString();
				break;
			}
		}
		Collectd.logInfo("Storm Nimbus Plugin: using following Nimbuses:" + nimbusAddresses);
		Collectd.logInfo("Storm Nimbus Plugin: using kerberos:" + kerberos);

		builder = HttpClientBuilder.create();
		context = HttpClientContext.create();
		if (kerberos) {
			System.setProperty("java.security.auth.login.config", jaasPath);
			System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
			System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");

			try {
				LoginContext ctx = new LoginContext("KrbLogin");
				ctx.login();
				subject = ctx.getSubject();
				System.out.println("Logged in");
			} catch (LoginException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create()
					.register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build();
			builder.setDefaultAuthSchemeRegistry(authSchemeRegistry);

			BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();

			// This may seem odd, but specifying 'null' as principal tells java
			// to
			// use the logged in user's credentials
			Credentials useJaasCreds = new Credentials() {

				public String getPassword() {
					return null;
				}

				public Principal getUserPrincipal() {
					return null;
				}

			};
			credentialsProvider.setCredentials(new AuthScope(null, -1, null), useJaasCreds);
			context.setCredentialsProvider(credentialsProvider);

		} else {
			subject = Subject.getSubject(AccessController.getContext());
		}
		return 0;
	}

	@Override
	public int shutdown() {
		return 0;
	}

}
