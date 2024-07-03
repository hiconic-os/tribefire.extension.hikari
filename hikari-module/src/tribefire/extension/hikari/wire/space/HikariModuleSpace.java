// ============================================================================
// Copyright BRAINTRIBE TECHNOLOGY GMBH, Austria, 2002-2022
//
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
package tribefire.extension.hikari.wire.space;

import static com.braintribe.wire.api.scope.InstanceConfiguration.currentInstance;

import com.braintribe.gm.model.reason.Maybe;
import com.braintribe.model.deployment.database.connector.DatabaseConnectionDescriptor;
import com.braintribe.model.deployment.database.pool.HikariCpConnectionPool;
import com.braintribe.model.processing.deployment.api.ExpertContext;
import com.braintribe.model.processing.deployment.api.binding.DenotationBindingBuilder;
import com.braintribe.utils.StringTools;
import com.braintribe.wire.api.annotation.Import;
import com.braintribe.wire.api.annotation.Managed;
import com.zaxxer.hikari.HikariDataSource;

import tribefire.extension.hikari.processing.HikariDataSources;
import tribefire.extension.hikari.processing.HikariDatabaseInfoProvider;
import tribefire.module.api.DenotationEnrichmentResult;
import tribefire.module.api.DenotationTransformationContext;
import tribefire.module.api.DenotationTransformerRegistry;
import tribefire.module.wire.contract.TribefireModuleContract;
import tribefire.module.wire.contract.TribefireWebPlatformContract;

/**
 * Binds the {@link HikariCpConnectionPool} deployable to {@link HikariDataSource}.
 */
@Managed
public class HikariModuleSpace implements TribefireModuleContract {

	@Import
	private TribefireWebPlatformContract tfPlatform;

	@Override
	public void bindHardwired() {
		DenotationTransformerRegistry registry = tfPlatform.hardwiredExperts().denotationTransformationRegistry();

		registry.registerStandardMorpher(DatabaseConnectionDescriptor.T, HikariCpConnectionPool.T, this::connectionDescriptorToHikariConnectionPool);

		registry.registerEnricher("HikariCpEdr2ccEnricher", HikariCpConnectionPool.T, this::enrichConnectionPool);
	}

	private Maybe<HikariCpConnectionPool> connectionDescriptorToHikariConnectionPool(DenotationTransformationContext context,
			DatabaseConnectionDescriptor cd) {

		HikariCpConnectionPool connectionPool = context.create(HikariCpConnectionPool.T);
		connectionPool.setConnectionDescriptor(cd);

		return Maybe.complete(connectionPool);
	}

	private DenotationEnrichmentResult<HikariCpConnectionPool> enrichConnectionPool(DenotationTransformationContext context,
			HikariCpConnectionPool cp) {
		String denotationId = context.denotationId();
		if (denotationId == null)
			return DenotationEnrichmentResult.nothingNowOrEver();

		StringBuilder sb = new StringBuilder();

		if (cp.getName() == null) {
			String prettyName = StringTools.convertCase(denotationId) //
					.splitOnDelimiter("-") //
					.capitalizeAll() //
					.join(" ");

			String name = prettyName + " Hikari Connection Pool";

			cp.setName(name);
			sb.append(" name to [" + name + "]");
		}

		if (cp.getExternalId() == null) {
			String externalId = "edr2cc:" + denotationId + ":hikari";

			cp.setExternalId(externalId);
			sb.append(" externalId to [" + externalId + "]");
		}

		if (cp.getGlobalId() == null) {
			cp.setGlobalId(cp.getExternalId());
			sb.append(" globalId to [" + cp.getExternalId() + "]");
		}

		if (sb.length() == 0)
			return DenotationEnrichmentResult.nothingNowOrEver();
		else
			return DenotationEnrichmentResult.allDone(cp, "Configured " + sb.toString());
	}

	@Override
	public void bindDeployables(DenotationBindingBuilder bindings) {
		bindings.bind(HikariCpConnectionPool.T) //
				.component(tfPlatform.binders().databaseConnectionPool())//
				/**/ .expertFactory(this::hikariCpConnector). //
				component(tfPlatform.binders().databaseConnectionInfoProvider()) //
				/**/ .expertFactory(this::hikariDatabaseConnectionInfoProvider);
	}

	@Managed
	private HikariDataSource hikariCpConnector(ExpertContext<HikariCpConnectionPool> context) {
		HikariCpConnectionPool deployable = context.getDeployable();

		HikariDataSource bean = hikariDataSources().dataSource(deployable);
		currentInstance().onDestroy(bean::close);

		return bean;
	}

	@Managed
	private HikariDatabaseInfoProvider hikariDatabaseConnectionInfoProvider(ExpertContext<HikariCpConnectionPool> context) {
		HikariCpConnectionPool deployable = context.getDeployable();

		HikariDatabaseInfoProvider bean = new HikariDatabaseInfoProvider();
		bean.setExternalId(deployable.getExternalId());
		bean.setMetricRegistry(hikariDataSources().metricRegistry());
		bean.setCortexSessionSupplier(tfPlatform.systemUserRelated().cortexSessionSupplier());

		return bean;
	}

	@Managed
	private HikariDataSources hikariDataSources() {
		return new HikariDataSources();
	}

}
