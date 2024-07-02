// ============================================================================
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ============================================================================
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

import static com.braintribe.utils.lcd.CollectionTools2.first;

import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import com.braintribe.cfg.Required;
import com.braintribe.gm.model.reason.Maybe;
import com.braintribe.gm.model.reason.essential.InternalError;
import com.braintribe.gm.model.reason.essential.NotFound;
import com.braintribe.logging.Logger;
import com.braintribe.model.deployment.Deployable;
import com.braintribe.model.deployment.database.connector.DatabaseConnectionDescriptor;
import com.braintribe.model.deployment.database.pool.HikariCpConnectionPool;
import com.braintribe.model.generic.pr.criteria.TraversingCriterion;
import com.braintribe.model.generic.processing.pr.fluent.TC;
import com.braintribe.model.platformreflection.db.DatabaseConnectionInfo;
import com.braintribe.model.platformreflection.db.DatabaseConnectionPoolMetrics;
import com.braintribe.model.processing.query.fluent.EntityQueryBuilder;
import com.braintribe.model.processing.session.api.persistence.PersistenceGmSession;
import com.braintribe.model.query.EntityQuery;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import tribefire.module.api.DatabaseConnectionInfoProvider;

public class HikariDatabaseInfoProvider implements DatabaseConnectionInfoProvider {

	private static final Logger log = Logger.getLogger(HikariDatabaseInfoProvider.class);

	private String externalId;
	private MetricRegistry metricRegistry;
	private Supplier<PersistenceGmSession> cortexSessionSupplier;

	@Required
	public void setExternalId(String externalId) {
		this.externalId = externalId;
	}

	@Required
	public void setMetricRegistry(MetricRegistry metricRegistry) {
		this.metricRegistry = metricRegistry;
	}

	@Required
	public void setCortexSessionSupplier(Supplier<PersistenceGmSession> cortexSessionSupplier) {
		this.cortexSessionSupplier = cortexSessionSupplier;
	}

	@Override
	public Maybe<DatabaseConnectionInfo> getDatabaseConnectionInfo() {
		Maybe<MBeanServer> maybeMBeanServer = getMBeanServer();
		if (maybeMBeanServer.isEmpty())
			return maybeMBeanServer.cast();

		MBeanServer mBeanServer = maybeMBeanServer.get();

		Maybe<ObjectName> maybeObjectName = queryObjectName(mBeanServer);
		if (maybeObjectName.isEmpty())
			return maybeObjectName.cast();

		return provideDbConnectionInfo(maybeObjectName.get(), mBeanServer);
	}

	private Maybe<MBeanServer> getMBeanServer() {
		try {
			return Maybe.complete(ManagementFactory.getPlatformMBeanServer());
		} catch (Error e) {
			log.debug(() -> "Could not access platform MBean server.", e);
			return InternalError.from(e, "Platform MBean Server could not be accessed.").asMaybe();
		}

	}

	private Maybe<ObjectName> queryObjectName(MBeanServer mBeanServer) {
		String pattern = "com.zaxxer.hikari:type=Pool (" + externalId + ")";
		try {
			ObjectName namePattern = new ObjectName(pattern);
			Set<ObjectName> names = mBeanServer.queryNames(namePattern, null);

			switch (names.size()) {
				case 0:
					return NotFound.create("Not MBean name found for pattern: " + pattern).asMaybe();
				case 1:
					return completeWithFirst(names);
				default:
					log.debug("Multiple MBeans found for pattern: " + pattern + ". Will provide info only based on first one.");
					return completeWithFirst(names);
			}

		} catch (MalformedObjectNameException e) {
			throw new RuntimeException("Problem with pattern: " + pattern, e);
		}

	}

	private Maybe<ObjectName> completeWithFirst(Set<ObjectName> names) {
		return Maybe.complete(first(names));
	}

	private Maybe<DatabaseConnectionInfo> provideDbConnectionInfo(ObjectName on, MBeanServer mBeanServer) {
		DatabaseConnectionInfo result = DatabaseConnectionInfo.T.create();
		result.setName(externalId);

		fillConfig(result, on, mBeanServer);
		enrichWithDescriptor(result);

		enrichWithMetrics(result, on, mBeanServer);

		return Maybe.complete(result);

	}

	private void enrichWithMetrics(DatabaseConnectionInfo dci, ObjectName on, MBeanServer mbeanServer) {
		DatabaseConnectionPoolMetrics metrics = DatabaseConnectionPoolMetrics.T.create();
		dci.setMetrics(metrics);

		boolean useMBeanServer = true;

		Map<String, Timer> timers = metricRegistry.getTimers();
		Timer waitTimer = timers.get(externalId + ".pool.Wait");
		if (waitTimer != null) {
			metrics.setWaitTimeOneMinuteRate(waitTimer.getOneMinuteRate());
			metrics.setWaitTimeFiveMinutesRate(waitTimer.getFiveMinuteRate());
			metrics.setWaitTimeFifteenMinutesRate(waitTimer.getFifteenMinuteRate());
			metrics.setWaitTimeMeanRate(waitTimer.getMeanRate());
			metrics.setLeaseCount(waitTimer.getCount());
		}

		Map<String, Histogram> histograms = metricRegistry.getHistograms();
		Histogram usageHistogram = histograms.get(externalId + ".pool.Usage");
		if (usageHistogram != null) {
			Snapshot snapshot = usageHistogram.getSnapshot();
			metrics.setUsageMinTime(snapshot.getMin());
			metrics.setUsageMaxTime(snapshot.getMax());
			metrics.setUsageMedianTime(snapshot.getMedian());
			metrics.setUsageMeanTime(snapshot.getMean());
		}

		@SuppressWarnings("rawtypes")
		Map<String, Gauge> gauges = metricRegistry.getGauges();
		Gauge<Integer> totalConnectionsGauge = gauges.get(externalId + ".pool.TotalConnections");
		Gauge<Integer> idleConnectionsGauge = gauges.get(externalId + ".pool.IdleConnections");
		Gauge<Integer> activeConnectionsGauge = gauges.get(externalId + ".pool.ActiveConnections");
		Gauge<Integer> pendingConnectionsGauge = gauges.get(externalId + ".pool.PendingConnections");

		if (totalConnectionsGauge != null && idleConnectionsGauge != null && activeConnectionsGauge != null && pendingConnectionsGauge != null) {

			Integer totalConnectionsValue = totalConnectionsGauge.getValue();
			Integer idleConnectionsValue = idleConnectionsGauge.getValue();
			Integer activeConnectionsValue = activeConnectionsGauge.getValue();
			Integer pendingConnectionsValue = pendingConnectionsGauge.getValue();

			metrics.setTotalConnections(totalConnectionsValue);
			metrics.setIdleConnections(idleConnectionsValue);
			metrics.setActiveConnections(activeConnectionsValue);
			metrics.setThreadsAwaitingConnections(pendingConnectionsValue);

			useMBeanServer = false;
		}

		if (useMBeanServer)
			try {
				enrichWithMBeanServer(on, mbeanServer, metrics);
			} catch (InstanceNotFoundException | ReflectionException e) {
				log.debug("Error while getting connection attributes for MBean: " + on.getCanonicalName(), e);
			}
	}

	private void enrichWithMBeanServer(ObjectName on, MBeanServer mbeanServer, DatabaseConnectionPoolMetrics metrics)
			throws InstanceNotFoundException, ReflectionException {

		AttributeList al = mbeanServer.getAttributes(on,
				new String[] { "ActiveConnections", "IdleConnections", "ThreadsAwaitingConnection", "TotalConnections" });
		if (al != null && al.size() > 0) {
			for (Iterator<Object> it = al.iterator(); it.hasNext();) {
				Object o = it.next();
				if (o instanceof Attribute) {
					Attribute a = (Attribute) o;

					switch (a.getName()) {
						case "ActiveConnections":
							metrics.setActiveConnections(getIntValue(a));
							break;
						case "IdleConnections":
							metrics.setIdleConnections(getIntValue(a));
							break;
						case "ThreadsAwaitingConnection":
							metrics.setThreadsAwaitingConnections(getIntValue(a));
							break;
						case "TotalConnections":
							metrics.setTotalConnections(getIntValue(a));
							break;
						default:
							break;
					}
				}
			}
		}
	}

	protected void enrichWithDescriptor(DatabaseConnectionInfo dci) {
		HikariCpConnectionPool cp = findHikariPool();
		if (cp == null)
			return;

		DatabaseConnectionDescriptor desc = cp.getConnectionDescriptor();
		if (desc == null)
			return;

		dci.setConnectionDescription(desc.describeConnection());
	}

	private HikariCpConnectionPool findHikariPool() {
		EntityQuery query = EntityQueryBuilder.from(HikariCpConnectionPool.T) //
				.where() //
				/**/ .property(Deployable.externalId).eq(externalId) //
				.tc(allTc()) //
				.done();

		PersistenceGmSession session = cortexSessionSupplier.get();
		return session.query().entities(query).unique();
	}

	private TraversingCriterion allTc() {
		return TC.create().negation().joker().done();
	}

	private void fillConfig(DatabaseConnectionInfo dci, ObjectName on, MBeanServer mBeanServer) {
		try {
			AttributeList al = mBeanServer.getAttributes(on,
					new String[] { "ConnectionTimeout", "IdleTimeout", "MaxLifetime", "MaximumPoolSize", "MinimumIdle", "ValidationTimeout" });

			if (al != null && al.size() > 0) {
				for (Iterator<Object> it = al.iterator(); it.hasNext();) {
					Object o = it.next();
					if (o instanceof Attribute) {
						Attribute a = (Attribute) o;

						switch (a.getName()) {
							case "ConnectionTimeout":
								dci.setConnectionTimeout(getLongValue(a));
								break;
							case "IdleTimeout":
								dci.setIdleTimeout(getLongValue(a));
								break;
							case "MaxLifetime":
								dci.setMaxLifetime(getLongValue(a));
								break;
							case "MaximumPoolSize":
								dci.setMaximumPoolSize(getIntValue(a));
								break;
							case "MinimumIdle":
								dci.setMinimumPoolSize(getIntValue(a));
								break;
							default:
								break;
						}
					}
				}
			}
		} catch (Exception e) {
			log.debug(() -> "Could not access the pool config for " + externalId, e);
		}
	}

	private Integer getIntValue(Attribute attr) {
		return getIntValue(attr.getValue());
	}

	private Integer getIntValue(Object v) {
		return v instanceof Integer ? (Integer) v : null;
	}

	private Long getLongValue(Attribute attr) {
		return getLongValue(attr.getValue());
	}

	private Long getLongValue(Object v) {
		return v instanceof Long ? (Long) v : null;
	}

}
