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
package io.prestosql.sql.planner.optimizations;


import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.JoinNode.EquiJoinClause;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Join;
import io.prestosql.sql.tree.SymbolReference;

import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;

public final class JoinNodeUtils
{
    private JoinNodeUtils() {}

    public static ComparisonExpression toExpression(EquiJoinClause clause)
    {
        return new ComparisonExpression(EQUAL, new SymbolReference(clause.getLeft().getName()), new SymbolReference(clause.getRight().getName()));
    }

    public static JoinNode.Type typeConvert(Join.Type joinType)
    {
        // Omit SEMI join types because they must be inferred by the planner and not part of the SQL parse tree
        switch (joinType) {
            case CROSS:
            case IMPLICIT:
            case INNER:
                return INNER;
            case LEFT:
                return LEFT;
            case RIGHT:
                return RIGHT;
            case FULL:
                return FULL;
            default:
                throw new UnsupportedOperationException("Unsupported join type: " + joinType);
        }
    }
}
