package com.dailycred.dynamonito.cache.elastic;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.ClientMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;

public class MemcachedClientProvider {

	private static final ClientMode CLIENT_MODE = ClientMode.Static;
	private static final String AWS_CREDENTIALS_PROPERTIES = "aws_credential.properties";
	private static final String AWS_ENDPOINT_PROPERTY_NAME = "elastiCacheConfigEndpoint";

	protected MemcachedClientIF client;

	public MemcachedClientProvider() {
		try {
			InputStream is = MemcachedClientProvider.class.getClassLoader()
					.getResourceAsStream(AWS_CREDENTIALS_PROPERTIES);
			Properties prop = new Properties();
			prop.load(is);
			String url = prop.getProperty(AWS_ENDPOINT_PROPERTY_NAME);
			client = new MemcachedClient(new BinaryConnectionFactory(CLIENT_MODE),
					AddrUtil.getAddresses(url));
		} catch (IOException e) {
			client = null;
			throw new RuntimeException(e);
		}
	}

	public MemcachedClientIF get() {
		return client;
	}

}
