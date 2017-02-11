# Storm Collectd
This is a Collectd input plugin to fetch all Topology Storm Nimbus UI metrics via REST API. 

**This plugin also supports Storm with Kerberos authentication enabled.**

### Build

```mvn clean package assembly:single```

### Configuration
```xml
<Plugin "storm">
	address "http://<nimbus1>:8744/"
	address "http://<nimbus2>:8744/"
	address "http://<nimbus3>:8744/"
	kerberos true
	jaas "<path to jaas.conf>/jaas.conf"
</Plugin>
```

```bash
/opt/collectd/sbin/collectd -f -C ./collectd.conf
```

##### For Kerberos
If you have Kerberos enabled in your Storm environment set the kerberos flag to **true** in the Collectd configuration for this plugin.

Additionally please supply a JAAS conf file to the plugin which provides details on the Kerberos Principal and Keytab this plugin should use for SPNEGO authentication against Storm Nimbus REST API.

### Sample Configuration
Sample configuration can be found in src/test/resources
