// ============================================================================
// Copyright BRAINTRIBE TECHNOLOGY GMBH, Austria, 2002-2022
// 
// This library is free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License along with this library; See http://www.gnu.org/licenses/.
// ============================================================================
package tribefire.extension.hikari.processing;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import com.braintribe.logging.Logger;
import com.braintribe.model.deployment.database.JdbcTransactionIsolationLevel;
import com.braintribe.model.deployment.database.connector.DatabaseConnectionDescriptor;
import com.braintribe.model.deployment.database.connector.GenericDatabaseConnectionDescriptor;
import com.braintribe.model.deployment.database.pool.HikariCpConnectionPool;
import com.braintribe.utils.lcd.CollectionTools;
import com.codahale.metrics.MetricRegistry;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.dropwizard.CodahaleMetricsTrackerFactory;

/**
 * @author peter.gazdik
 */
public class HikariDataSources {

	private static final Logger log = Logger.getLogger(HikariDataSources.class);

	private final ConcurrentHashMap<String, DataSource> createdDataSources = new ConcurrentHashMap<>();

	private final MetricRegistry metricRegistry = new MetricRegistry();

	/**
	 * <p>
	 * Creates a HikariCP {@link HikariDataSource} based on the given {@link HikariCpConnectionPool}.
	 * 
	 * @param connectionPool
	 *            The {@link HikariCpConnectionPool} providing the necessary configuration for creating a HikariCP {@link HikariDataSource}.
	 * @return A HikariCP {@link HikariDataSource} created based on the given {@link HikariCpConnectionPool}
	 */
	public synchronized HikariDataSource dataSource(HikariCpConnectionPool connectionPool) {

		String key = connectionPool.getExternalId();
		Objects.requireNonNull(key, "The connection pool " + connectionPool.stringify() + " does not have an externalId.");

		HikariDataSource existingDataSource = (HikariDataSource) createdDataSources.get(key);
		if (existingDataSource != null) {
			if (existingDataSource.isClosed()) {
				createdDataSources.remove(key);
			} else {
				return existingDataSource;
			}
		}

		DatabaseConnectionDescriptor originalConnector = connectionPool.getConnectionDescriptor();
		GenericDatabaseConnectionDescriptor connector = GenericDatabaseConnectionDescriptor.from(originalConnector);

		String url = connector.getUrl();

		StringBuilder sb = new StringBuilder("HikariCP:'");
		if (url != null) {
			sb.append(url);
		}
		sb.append('\'');
		log.pushContext(sb.toString());

		try {

			HikariDataSource result = new HikariDataSource();

			result.setDriverClassName(connector.getDriver());
			result.setJdbcUrl(url);
			result.setUsername(connector.getUser());
			result.setPassword(connector.getPassword());

			configurePooledJdbcConnectorSettings(connectionPool, result);
			configureExtendedConnectionPoolSettings(connectionPool, result);
			configureInitStatements(connectionPool, result);

			createdDataSources.put(key, result);

			return result;

		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid config: " + connectionPool, e);
		} finally {
			log.popContext();
		}

	}

	private static void configurePooledJdbcConnectorSettings(HikariCpConnectionPool connectionPool, HikariDataSource result) {

		boolean debug = log.isDebugEnabled();

		Integer checkoutTimeout = connectionPool.getCheckoutTimeout();
		if (checkoutTimeout != null) {
			if (debug)
				log.debug("Checkout timeout: " + checkoutTimeout + " ms");
			result.setConnectionTimeout(checkoutTimeout);
		}

		Integer maxPoolSize = connectionPool.getMaxPoolSize();
		if (maxPoolSize != null && maxPoolSize > 0) {
			if (debug)
				log.debug("Maximum poolsize: " + maxPoolSize);
			result.setMaximumPoolSize(maxPoolSize);
		}

		Integer minPoolSize = connectionPool.getMinPoolSize();
		if (minPoolSize != null && minPoolSize >= 0) {
			if (debug)
				log.debug("Minimum poolsize: " + minPoolSize);
			result.setMinimumIdle(minPoolSize);
		}

		if ((maxPoolSize != null && maxPoolSize > 0) && (minPoolSize == null || minPoolSize == 0)) {
			log.info("MaxPoolSize is set to " + maxPoolSize
					+ " while no minPoolSize is defined. Please be aware that this will immediately open (and keep) " + maxPoolSize
					+ " connections.");
		}

		String preferredTestQuery = connectionPool.getPreferredTestQuery();
		if (preferredTestQuery != null) {
			if (debug)
				log.debug("Test query: " + preferredTestQuery);
			result.setConnectionTestQuery(preferredTestQuery);
		}

		Integer maxStatements = connectionPool.getMaxStatements();
		if (maxStatements != null) {
			if (debug)
				log.debug("Maximum statements cached: " + maxStatements);
			result.addDataSourceProperty("cachePrepStmts", "true");
			result.addDataSourceProperty("prepStmtCacheSize", "" + maxStatements);
		}

		Integer maxIdleTime = connectionPool.getMaxIdleTime();
		if (maxIdleTime != null) {
			if (debug)
				log.debug("Maximum idle time: " + maxIdleTime + " ms");
			result.setIdleTimeout(maxIdleTime);
		}

		JdbcTransactionIsolationLevel isolationLevel = connectionPool.getTransactionIsolationLevel();
		if (isolationLevel != null)
			result.setTransactionIsolation("TRANSACTION_" + isolationLevel.name());
	}

	private void configureExtendedConnectionPoolSettings(HikariCpConnectionPool connectionPool, HikariDataSource result) throws SQLException {

		boolean debug = log.isDebugEnabled();

		Integer loginTimeout = connectionPool.getLoginTimeout();
		if (loginTimeout != null) {
			if (debug)
				log.debug("Login timeout: " + loginTimeout + " ms");
			result.setLoginTimeout(loginTimeout);

		}

		Integer initializationFailTimeout = connectionPool.getInitializationFailTimeout();
		if (initializationFailTimeout != null) {
			if (debug)
				log.debug("Initialization timeout: " + initializationFailTimeout + " ms");
			result.setInitializationFailTimeout(initializationFailTimeout);
		}

		Integer validationTimeout = connectionPool.getValidationTimeout();
		if (validationTimeout != null) {
			if (debug)
				log.debug("Maximum validation timeout: " + validationTimeout + " ms");
			result.setValidationTimeout(validationTimeout);
		}

		String dataSourceName = connectionPool.getDataSourceName();
		if (dataSourceName != null) {
			if (debug)
				log.debug("Datasource name: " + dataSourceName);
			result.setPoolName(dataSourceName);

		} else {
			String extId = connectionPool.getExternalId();
			if (extId != null) {
				extId = extId.replace(':', '/'); // ':' is forbidden in pool names (at least sometimes)

				if (debug)
					log.debug("Datasource name (from external Id): " + extId);
				result.setPoolName(extId);
			}
		}

		String schema = connectionPool.getSchema();
		if (schema != null) {
			if (debug)
				log.debug("Schema: " + schema);
			result.setSchema(schema);
		}

		boolean enableJmx = connectionPool.getEnableJmx();
		if (enableJmx) {
			result.setRegisterMbeans(true);
		}

		boolean enableMetrics = connectionPool.getEnableMetrics();
		if (enableMetrics) {
			try {
				MetricsTrackerFactory metricsTrackerFactory = new CodahaleMetricsTrackerFactory(metricRegistry);
				result.setMetricsTrackerFactory(metricsTrackerFactory);
			} catch (Exception e) {
				log.warn(() -> "Could not enable metrics for connection pool " + connectionPool, e);
			}
		}
	}

	private static void configureInitStatements(HikariCpConnectionPool connectionPool, HikariDataSource result) {
		if (CollectionTools.isEmpty(connectionPool.getInitialStatements())) {
			return;
		}

		List<String> initialStatements = connectionPool.getInitialStatements();
		StringBuilder sb = new StringBuilder();
		for (String st : initialStatements) {
			sb.append(st);
			sb.append(";\n");
		}

		if (log.isDebugEnabled()) {
			log.debug("Connection initialization statement: " + sb.toString());
		}
		result.setConnectionInitSql(sb.toString());
	}

	public MetricRegistry metricRegistry() {
		return metricRegistry;
	}

}
