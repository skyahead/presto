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
package io.prestosql.sql.planner.iterative.rule;

import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.Symbol;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.sql.planner.iterative.Rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.Patterns.*;

public class PushAggregationIntoTableScan
        implements Rule<AggregationNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(tableScan().capturedAs(TABLE_SCAN)));

    private final Metadata metadata;

    public PushAggregationIntoTableScan(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        // TODO: to confirm, do we not support partial aggregation?
        if (aggregationNode.getStep().isOutputPartial()) {
            return Result.empty();
        }

        return metadata.applyAggregation(context.getSession(), tableScan.getTable(), false, tableScan.getAssignments(), aggregationNode)
                .map(result -> {
                    List<Symbol> outputSymbols = new ArrayList<>();
                    outputSymbols.addAll(aggregationNode.getGroupingKeys());
                    outputSymbols.addAll(aggregationNode.getAggregations().keySet());

                    Map<Symbol, ColumnHandle> assignments = new HashMap<>();
                    for (Symbol symbol: aggregationNode.getGroupingKeys()) {
                        assignments.put(symbol, tableScan.getAssignments().get(symbol));
                    }
                    assignments.putAll(result.getAssignments());

                    PlanNode node = new TableScanNode(
                            tableScan.getId(),
                            result.getHandle(),
                            outputSymbols,
                            assignments,
                            tableScan.getEnforcedConstraint());

                    return Result.ofPlanNode(node);
                })
                .orElseGet(Result::empty);
    }
}
