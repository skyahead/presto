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
package io.prestosql.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Symbol;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.AggregationNode.GroupingSetDescriptor;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.predicate.TupleDomain;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public final class JdbcTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;

    // catalog, schema and table names are reported by the remote database
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final Optional<List<JdbcColumnHandle>> columns;
    private final TupleDomain<ColumnHandle> constraint;
    private final OptionalLong limit;
    private final Optional<Map<Symbol, Aggregation>> aggregations;
    private final Optional<GroupingSetDescriptor> groupingSets;
    private final Optional<OrderingScheme> orderingScheme;

    public JdbcTableHandle(SchemaTableName schemaTableName, @Nullable String catalogName, @Nullable String schemaName, String tableName)
    {
        this(schemaTableName, catalogName, schemaName, tableName, Optional.empty(), TupleDomain.all(), OptionalLong.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    @JsonCreator
    public JdbcTableHandle(
        @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
        @JsonProperty("catalogName") @Nullable String catalogName,
        @JsonProperty("schemaName") @Nullable String schemaName,
        @JsonProperty("tableName") String tableName,
        @JsonProperty("columns") Optional<List<JdbcColumnHandle>> columns,
        @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
        @JsonProperty("limit") OptionalLong limit,
        @JsonProperty("aggregations") Optional<Map<Symbol, Aggregation>> aggregations,
        @JsonProperty("groupingSets") Optional<GroupingSetDescriptor> groupingSets,
        @JsonProperty("orderingScheme") Optional<OrderingScheme> orderingScheme)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");
        requireNonNull(columns, "columns is null");
        this.columns = columns.map(ImmutableList::copyOf);
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.aggregations = aggregations;
        this.groupingSets = groupingSets;
        this.orderingScheme = orderingScheme;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    @Nullable
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Optional<List<JdbcColumnHandle>> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public Optional<Map<Symbol, Aggregation>> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty
    public Optional<GroupingSetDescriptor> getGroupingSets()
    {
        return groupingSets;
    }

    @JsonProperty
    public Optional<OrderingScheme> getOrderingScheme()
    {
        return orderingScheme;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        JdbcTableHandle o = (JdbcTableHandle) obj;
        return Objects.equals(this.schemaTableName, o.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(schemaTableName).append(" ");
        Joiner.on(".").skipNulls().appendTo(builder, catalogName, schemaName, tableName);
        columns.ifPresent(value -> builder.append(" columns=").append(value));
        limit.ifPresent(value -> builder.append(" limit=").append(value));
        aggregations.ifPresent(agg -> {
            builder.append(" aggregations: ");
            agg.forEach((key, value) -> builder.append(" key=").append(key).append(", value=").append(value));
        });
        groupingSets.ifPresent(gs -> {
            builder.append(" groupingSets: ").append(gs.getGroupingKeys());
        });
        orderingScheme.ifPresent(os -> {
            builder.append(" orderingScheme: ").append(os.getOrderingList());
        });
        return builder.toString();
    }
}
