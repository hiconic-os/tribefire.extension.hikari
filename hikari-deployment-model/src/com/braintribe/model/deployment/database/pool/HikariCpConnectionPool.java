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
package com.braintribe.model.deployment.database.pool;

import java.util.List;

import com.braintribe.model.deployment.database.JdbcTransactionIsolationLevel;
import com.braintribe.model.generic.annotation.Initializer;
import com.braintribe.model.generic.reflection.EntityType;
import com.braintribe.model.generic.reflection.EntityTypes;

public interface HikariCpConnectionPool extends ConfiguredDatabaseConnectionPool, DatabaseConnectionInfoProvider {

	EntityType<HikariCpConnectionPool> T = EntityTypes.T(HikariCpConnectionPool.class);

	@Initializer("60000")
	Integer getCheckoutTimeout();
	void setCheckoutTimeout(Integer checkoutTimeout);

	@Initializer("3")
	Integer getMaxPoolSize();
	void setMaxPoolSize(Integer maxPoolSize);

	@Initializer("1")
	Integer getMinPoolSize();
	void setMinPoolSize(Integer minPoolSize);

	String getPreferredTestQuery();
	void setPreferredTestQuery(String preferredTestQuery);

	Integer getMaxStatements();
	void setMaxStatements(Integer maxStatements);

	@Initializer("60000")
	Integer getMaxIdleTime();
	void setMaxIdleTime(Integer maxIdleTime);

	@Initializer("10")
	Integer getLoginTimeout();
	void setLoginTimeout(Integer loginTimeout);

	String getDataSourceName();
	void setDataSourceName(String dataSourceName);

	List<String> getInitialStatements();
	void setInitialStatements(List<String> initialStatements);

	@Initializer("true")
	boolean getEnableJmx();
	void setEnableJmx(boolean enableJmx);

	@Initializer("true")
	boolean getEnableMetrics();
	void setEnableMetrics(boolean enableMetrics);

	@Initializer("60000")
	Integer getValidationTimeout();
	void setValidationTimeout(Integer validationTimeout);

	Integer getInitializationFailTimeout();
	void setInitializationFailTimeout(Integer initializationFailTimeout);

	String getSchema();
	void setSchema(String schema);

	JdbcTransactionIsolationLevel getTransactionIsolationLevel();
	void setTransactionIsolationLevel(JdbcTransactionIsolationLevel transactionIsolationLevel);

	/* Integer getAcquireIncrement(); void setAcquireIncrement(Integer acquireIncrement);
	 * 
	 * Integer getAcquireRetryAttempts(); void setAcquireRetryAttempts(Integer acquireRetryAttempts);
	 * 
	 * Integer getAcquireRetryDelay(); void setAcquireRetryDelay(Integer acquireRetryDelay);
	 * 
	 * Boolean getAutoCommitOnClose(); void setAutoCommitOnClose(Boolean autoCommitOnClose);
	 * 
	 * String getAutomaticTestTable(); void setAutomaticTestTable(String automaticTestTable);
	 * 
	 * Boolean getBreakAfterAcquireFailure(); void setBreakAfterAcquireFailure(Boolean breakAfterAcquireFailure);
	 * 
	 * 
	 * String getConnectionCustomizerClassName(); void setConnectionCustomizerClassName(String
	 * connectionCustomizerClassName);
	 * 
	 * String getConnectionTesterClassName(); void setConnectionTesterClassName(String connectionTesterClassName);
	 * 
	 * Boolean getDebugUnreturnedConnectionStackTraces(); void setDebugUnreturnedConnectionStackTraces(Boolean
	 * debugUnreturnedConnectionStackTraces);
	 * 
	 * String getPoolDescription(); void setPoolDescription(String description);
	 * 
	 * Boolean getForceIgnoreUnresolvedTransactions(); void setForceIgnoreUnresolvedTransactions(Boolean
	 * forceIgnoreUnresolvedTransactions);
	 * 
	 * Integer getIdleConnectionTestPeriod(); void setIdleConnectionTestPeriod(Integer idleConnectionTestPeriod);
	 * 
	 * Integer getInitialPoolSize(); void setInitialPoolSize(Integer initialPoolSize);
	 * 
	 * Integer getMaxAdministrativeTaskTime(); void setMaxAdministrativeTaskTime(Integer maxAdministrativeTaskTime);
	 * 
	 * Integer getMaxConnectionAge(); void setMaxConnectionAge(Integer maxConnectionAge);
	 * 
	 * 
	 * Integer getMaxIdleTimeExcessConnections(); void setMaxIdleTimeExcessConnections(Integer
	 * maxIdleTimeExcessConnections);
	 * 
	 * 
	 * 
	 * Integer getMaxStatementsPerConnection(); void setMaxStatementsPerConnection(Integer maxStatementsPerConnection);
	 * 
	 * 
	 * String getOverrideDefaultPassword(); void setOverrideDefaultPassword(String overrideDefaultPassword);
	 * 
	 * String getOverrideDefaultUser(); void setOverrideDefaultUser(String overrideDefaultUser);
	 * 
	 * 
	 * Integer getPropertyCycle(); void setPropertyCycle(Integer propertyCycle);
	 * 
	 * Boolean getTestConnectionOnCheckin(); void setTestConnectionOnCheckin(Boolean testConnectionOnCheckin);
	 * 
	 * Boolean getTestConnectionOnCheckout(); void setTestConnectionOnCheckout(Boolean testConnectionOnCheckout);
	 * 
	 * Integer getUnreturnedConnectionTimeout(); void setUnreturnedConnectionTimeout(Integer
	 * unreturnedConnectionTimeout);
	 * 
	 * Boolean getUsesTraditionalReflectiveProxies(); void setUsesTraditionalReflectiveProxies(Boolean
	 * usesTraditionalReflectiveProxies);
	 * 
	 * 
	 * 
	 * String getFactoryClassLocation(); void setFactoryClassLocation(String factoryClassLocation);
	 * 
	 * String getIdentityToken(); void setIdentityToken(String identityToken);
	 * 
	 * Integer getNumHelperThreads(); void setNumHelperThreads(Integer numHelperThreads); */

	// @formatter:on

}
