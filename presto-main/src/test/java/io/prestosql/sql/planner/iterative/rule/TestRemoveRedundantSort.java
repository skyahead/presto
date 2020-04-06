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

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestRemoveRedundantSort
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new RemoveRedundantSort())
                .on(p ->
                        p.sort(
                                ImmutableList.of(p.symbol("c")),
                                p.aggregation(builder -> builder
                                        .addAggregation(p.symbol("c"), expression("count(foo)"), ImmutableList.of(BIGINT))
                                        .globalGrouping()
                                        .source(p.values(p.symbol("foo"))))))
                .matches(
                        node(AggregationNode.class,
                                node(ValuesNode.class)));
    }

    @Test
    public void testForZeroCardinality()
    {
        tester().assertThat(new RemoveRedundantSort())
                .on(p ->
                        p.sort(
                                ImmutableList.of(p.symbol("c")),
                                p.values(p.symbol("foo"))))
                .matches(node(ValuesNode.class));
    }

    @Test
    public void doesNotFire()
    {
        tester().assertThat(new RemoveRedundantSort())
                .on(p ->
                        p.sort(
                                ImmutableList.of(p.symbol("c")),
                                p.aggregation(builder -> builder
                                        .addAggregation(p.symbol("c"), expression("count(foo)"), ImmutableList.of(BIGINT))
                                        .singleGroupingSet(p.symbol("foo"))
                                        .source(p.values(20, p.symbol("foo"))))))
                .doesNotFire();
    }
}
