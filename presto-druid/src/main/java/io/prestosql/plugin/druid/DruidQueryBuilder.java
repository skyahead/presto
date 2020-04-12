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
import io.airlift.slice.Slice;
import io.prestosql.plugin.jdbc.*;
import io.prestosql.spi.Symbol;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.joda.time.DateTimeZone;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.joining;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Keeping this class till this is fixed: https://issues.apache.org/jira/browse/CALCITE-2873
 * Also `select null` throws error from druid side.
 */
public class DruidQueryBuilder extends QueryBuilder
{
    // not all databases support booleans, so use 1=1 and 1=0 instead
    private static final String ALWAYS_TRUE = "1=1";
    private static final String ALWAYS_FALSE = "1=0";

    private static final Logger LOG = Logger.get(DruidQueryBuilder.class);

    private final String quote;

    private static class TypeAndValue
    {
        private final Type type;
        private final JdbcTypeHandle typeHandle;
        private final Object value;

        public TypeAndValue(Type type, JdbcTypeHandle typeHandle, Object value)
        {
            this.type = requireNonNull(type, "type is null");
            this.typeHandle = requireNonNull(typeHandle, "typeHandle is null");
            this.value = requireNonNull(value, "value is null");
        }

        public Type getType()
        {
            return type;
        }

        public JdbcTypeHandle getTypeHandle()
        {
            return typeHandle;
        }

        public Object getValue()
        {
            return value;
        }
    }

    DruidQueryBuilder(String quote)
    {
        super(quote);
        this.quote = quote;
    }

    PreparedStatement buildSql(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            JdbcTableHandle tableHandle,
            List<JdbcColumnHandle> columns,
            Optional<String> additionalPredicate,
            Function<String, String> sqlFunction)
            throws SQLException {
        String catalog = tableHandle.getCatalogName();
        String schema = tableHandle.getSchemaName();
        String table = tableHandle.getTableName();
        TupleDomain<ColumnHandle> tupleDomain = tableHandle.getConstraint();

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");

        Set<String> aggColumnNames = new HashSet<>();
        Set<String> groupByColumnNames = new HashSet<>();
        for (JdbcColumnHandle jdbcColumnHandle : columns) {
            if (jdbcColumnHandle.getSymbol().isPresent()) {
                aggColumnNames.add(jdbcColumnHandle.getColumnName());
            } else {
                groupByColumnNames.add(jdbcColumnHandle.getColumnName());
            }
        }

        // non aggregation columns
        String columnNames = groupByColumnNames.stream().map(this::quote).collect(joining(", "));
        sql.append(columnNames);
        if (columns.isEmpty()) {
            sql.append("1"); // TODO: Select null does not work in druid-sql
        }

        if (tableHandle.getAggregations().isPresent()) {
            Map<Symbol, Aggregation> aggregationMap = tableHandle.getAggregations().get();

            for (Symbol aggFun : aggregationMap.keySet()) {
                Aggregation aggregation = aggregationMap.get(aggFun);

                String arguments = Joiner.on(", ").join(aggregation.getArguments());
                if (aggregation.getArguments().isEmpty() && "count".equalsIgnoreCase(aggregation.getResolvedFunction().getSignature().getName())) {
                    arguments = "*";
                }
                if (aggregation.isDistinct()) {
                    arguments = "DISTINCT " + arguments;
                }

                sql.append(", ");
                sql.append(aggregation.getResolvedFunction().getSignature().getName())
                    .append('(').append(arguments);

                aggregation.getOrderingScheme().ifPresent(orderingScheme -> sql.append(' ').append(orderingScheme.getOrderBy().stream()
                    .map(input -> input + " " + orderingScheme.getOrdering(input))
                    .collect(joining(", "))));

                sql.append(')');

//                aggregation.getFilter().ifPresent(expression -> sql.append(" FILTER (WHERE ").append(expression).append(")"));

                aggregation.getMask().ifPresent(symbol -> sql.append(" (mask = ").append(symbol).append(")"));
            }
        }

        // from
        sql.append(" FROM ");
        if (!isNullOrEmpty(catalog)) {
            sql.append(quote(catalog)).append('.');
        }
        if (!isNullOrEmpty(schema)) {
            sql.append(quote(schema)).append('.');
        }
        sql.append(quote(table));

        // where
        List<TypeAndValue> accumulator = new ArrayList<>();
        List<String> clauses = toConjuncts(client, session, tupleDomain, accumulator, connection);
        if (additionalPredicate.isPresent()) {
            clauses = ImmutableList.<String>builder()
                    .addAll(clauses)
                    .add(additionalPredicate.get())
                    .build();
        }
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ")
                .append(Joiner.on(" AND ").join(clauses));
        }

        // group by
        if (tableHandle.getGroupingSets().isPresent()) {
            sql.append(" GROUP BY ");

            String groupBy = tableHandle.getGroupingSets().get().getGroupingKeys()
                .stream()
                .map(symbol -> quote(symbol.getName()))
                .collect(joining(", "));

            sql.append(groupBy);
            sql.append(" ");
        }

        // limit
        String sqlPrepared = sqlFunction.apply(sql.toString());
        /* DRUID connector fails if values are not assigned. So we are going to build the SQL with values assigned */
        // TODO : cleanup this class once fixed https://issues.apache.org/jira/browse/CALCITE-2873
        for (int i = 0; i < accumulator.size(); i++) {
            sqlPrepared = assignVal(sqlPrepared, getStringValue(accumulator.get(i)));
        }

        LOG.info("sqlPrepared: " + sqlPrepared);

        return client.getPreparedStatement(connection, sqlPrepared);
    }

    private String assignVal(String sql, String val)
    {
        return sql.replaceFirst("\\?", "'" + val + "'");
    }

    private String getStringValue(TypeAndValue typeAndValue)
    {
        if (typeAndValue.getType().equals(BigintType.BIGINT)) {
            return String.valueOf((long) typeAndValue.getValue());
        }
        else if (typeAndValue.getType().equals(IntegerType.INTEGER)) {
            return String.valueOf((((Number) typeAndValue.getValue()).intValue()));
        }
        else if (typeAndValue.getType().equals(SmallintType.SMALLINT)) {
            return String.valueOf(((Number) typeAndValue.getValue()).shortValue());
        }
        else if (typeAndValue.getType().equals(TinyintType.TINYINT)) {
            return String.valueOf(((Number) typeAndValue.getValue()).byteValue());
        }
        else if (typeAndValue.getType().equals(DoubleType.DOUBLE)) {
            return String.valueOf((double) typeAndValue.getValue());
        }
        else if (typeAndValue.getType().equals(RealType.REAL)) {
            return String.valueOf(intBitsToFloat(((Number) typeAndValue.getValue()).intValue()));
        }
        else if (typeAndValue.getType().equals(BooleanType.BOOLEAN)) {
            return String.valueOf((boolean) typeAndValue.getValue());
        }
        else if (typeAndValue.getType().equals(DateType.DATE)) {
            long millis = DAYS.toMillis((long) typeAndValue.getValue());
            return String.valueOf(new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis)));
        }
        else if (typeAndValue.getType().equals(TimeType.TIME)) {
            return String.valueOf(new Time((long) typeAndValue.getValue()));
        }
        else if (typeAndValue.getType().equals(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE)) {
            return String.valueOf(new Time(unpackMillisUtc((long) typeAndValue.getValue())));
        }
        else if (typeAndValue.getType().equals(TimestampType.TIMESTAMP)) {
            return String.valueOf(new Timestamp((long) typeAndValue.getValue()));
        }
        else if (typeAndValue.getType().equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)) {
            return String.valueOf(new Timestamp(unpackMillisUtc((long) typeAndValue.getValue())));
        }
        else if (typeAndValue.getType() instanceof VarcharType) {
            return String.valueOf(((Slice) typeAndValue.getValue()).toStringUtf8());
        }
        else if (typeAndValue.getType() instanceof CharType) {
            return String.valueOf(((Slice) typeAndValue.getValue()).toStringUtf8());
        }
        else {
            throw new UnsupportedOperationException("Can't handle type: " + typeAndValue.getType());
        }
    }

    private static Domain pushDownDomain(JdbcClient client, ConnectorSession session, JdbcColumnHandle column, Domain domain, Connection connection)
    {
        return client.toPrestoType(session, connection, column.getJdbcTypeHandle())
                .orElseThrow(() -> new IllegalStateException(format("Unsupported type %s with handle %s", column.getColumnType(), column.getJdbcTypeHandle())))
                .getPushdownConverter().apply(domain);
    }

    private List<String> toConjuncts(JdbcClient client, ConnectorSession session, TupleDomain<ColumnHandle> tupleDomain, List<TypeAndValue> accumulator, Connection connection)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (tupleDomain.getDomains().isPresent()) {
            Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().get();
            for (ColumnHandle column : domains.keySet()) {
                JdbcColumnHandle jdbcColumnHandle = (JdbcColumnHandle) column;

                Domain domain = domains.get(jdbcColumnHandle);
                domain = pushDownDomain(client, session, jdbcColumnHandle, domain, connection);
                builder.add(toPredicate(jdbcColumnHandle.getColumnName(), domain, jdbcColumnHandle, accumulator));
            }
        }
        return builder.build();
    }

    private String toPredicate(String columnName, Domain domain, JdbcColumnHandle column, List<TypeAndValue> accumulator)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? quote(columnName) + " IS NULL" : ALWAYS_FALSE;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? ALWAYS_TRUE : quote(columnName) + " IS NOT NULL";
        }

        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(range.getLow().getValue());
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), column, accumulator));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), column, accumulator));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), column, accumulator));
                            break;
                        case BELOW:
                            rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), column, accumulator));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toPredicate(columnName, "=", getOnlyElement(singleValues), column, accumulator));
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                bindValue(value, column, accumulator);
            }
            String values = Joiner.on(",").join(nCopies(singleValues.size(), "?"));
            disjuncts.add(quote(columnName) + " IN (" + values + ")");
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(quote(columnName) + " IS NULL");
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String toPredicate(String columnName, String operator, Object value, JdbcColumnHandle column, List<TypeAndValue> accumulator)
    {
        bindValue(value, column, accumulator);
        return quote(columnName) + " " + operator + " ?";
    }

    private String quote(String name)
    {
        name = name.replace(quote, quote + quote);
        return quote + name + quote;
    }

    private static void bindValue(Object value, JdbcColumnHandle column, List<TypeAndValue> accumulator)
    {
        Type type = column.getColumnType();
        accumulator.add(new TypeAndValue(type, column.getJdbcTypeHandle(), value));
    }
}
