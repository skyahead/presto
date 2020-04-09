/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.druid;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import io.prestosql.spi.PrestoException;
import org.apache.calcite.avatica.remote.Driver;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

public class DruidJdbcClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(DruidJdbcClient.class).in(Scopes.SINGLETON);
        ensureCatalogIsEmpty(buildConfigObject(BaseJdbcConfig.class).getConnectionUrl());
        configBinder(binder).bindConfig(DruidConfig.class);
    }

    private static void ensureCatalogIsEmpty(String connectionUrl)
    {
        try {
            Driver driver = new Driver();
            DriverPropertyInfo[] driverPropertyInfo = driver.getPropertyInfo(connectionUrl, new Properties());
            checkArgument(connectionUrl != null, "Invalid JDBC URL for Druid connector");
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "Invalid configuration", e);
        }
    }


    @Provides
    @Singleton
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
    {
        return new DriverConnectionFactory(new Driver(), config, credentialProvider);
    }
}
