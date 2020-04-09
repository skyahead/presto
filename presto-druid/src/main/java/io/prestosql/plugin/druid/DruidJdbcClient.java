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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.Symbol;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.AggregationNode.GroupingSetDescriptor;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.util.stream.Collectors.joining;

public class DruidJdbcClient
  extends BaseJdbcClient
{
    private static final Logger LOG = Logger.get(DruidJdbcClient.class);

    @Inject
    public DruidJdbcClient(BaseJdbcConfig config,
                           DruidConfig druidConfig,
                           ConnectionFactory connectionFactory)
    {
        super(config, "\"", connectionFactory);
    }


    @Override
    public void abortReadConnection(Connection connection)
      throws SQLException
    {
        connection.abort(directExecutor());
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
      throws SQLException
    {
        LOG.info("getPreparedStatement sql: " + sql);
        PreparedStatement statement = connection.prepareStatement(sql);
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
      throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        // For druid - catalog is empty.
        return metadata.getTables("druid",
          schemaName.orElse(null),
          tableName.orElse(null),
          null);
    }

    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
      throws SQLException
    {
        // Druid table/schema is case sensitive.
        return new SchemaTableName(
          resultSet.getString("TABLE_SCHEM"),
          resultSet.getString("TABLE_NAME"));
    }

    @Override
    public List<SchemaTableName> getTableNames(JdbcIdentity identity, Optional<String> schema)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            try (ResultSet resultSet = getTables(connection, schema, Optional.empty())) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    list.add(getSchemaTableName(resultSet));
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            try (ResultSet resultSet = getTables(connection, Optional.of(jdbcSchemaName), Optional.of(jdbcTableName))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                      schemaTableName,
                      "",
                      resultSet.getString("TABLE_SCHEM"),
                      resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return Optional.empty();
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return Optional.of(getOnlyElement(tableHandles));
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        switch (typeHandle.getJdbcType()) {
            case Types.VARCHAR:
                return jdbcTypeToPrestoType(typeHandle);
            default:
                return super.toPrestoType(session, connection, typeHandle);
        }
    }

    private Optional<ColumnMapping> jdbcTypeToPrestoType(JdbcTypeHandle type)
    {
        int columnSize = type.getColumnSize();
        if (columnSize > VarcharType.MAX_LENGTH || columnSize == -1) {
            return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
        }
        return Optional.of(varcharColumnMapping(createVarcharType(columnSize)));
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            // Overriding this method because of the way we retrieve columns in druid.
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                      resultSet.getInt("DATA_TYPE"),
                      Optional.of(resultSet.getString("TYPE_NAME")),
                      resultSet.getInt("COLUMN_SIZE"),
                      resultSet.getInt("DECIMAL_DIGITS"),
                      Optional.of(0));
                    Optional<ColumnMapping> columnMapping = toPrestoType(session, connection, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        columns.add(new JdbcColumnHandle(columnName, typeHandle, columnMapping.get().getType(), nullable));
                    }
                }
                if (columns.isEmpty()) {
                    // In rare cases (e.g. PostgreSQL) a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
      throws SQLException
    {
        return metadata.getColumns(
          "druid",
          tableHandle.getSchemaName(),
          tableHandle.getTableName(),
          null);
    }

    /**
     * Overriding this method till this is fixed : https://issues.apache.org/jira/browse/CALCITE-2873
     * @param session
     * @param connection
     * @param split
     * @param table
     * @param columns
     * @return
     * @throws SQLException
     */
    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
      throws SQLException
    {
        OptionalLong forcedLimit = getForcedLimit(table.getLimit());

        return new DruidQueryBuilder(identifierQuote)
          .buildSql(
            this,
            session,
            connection,
            table,
            columns,
            split.getAdditionalPredicate(),
            tryApplyLimit(forcedLimit));
    }

    private OptionalLong getForcedLimit(OptionalLong optionalLong) {
        long FORCED_LIMIT = 10000L;

        OptionalLong forcedLimit = OptionalLong.of(FORCED_LIMIT);

        if (optionalLong.isPresent() && optionalLong.getAsLong() <= FORCED_LIMIT) {
            forcedLimit = optionalLong;
        }

        return forcedLimit;
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        return true;
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }
}
