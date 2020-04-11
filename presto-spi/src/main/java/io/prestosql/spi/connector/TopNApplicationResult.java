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
package io.prestosql.spi.connector;

import io.prestosql.spi.plan.OrderingScheme;

import static java.util.Objects.requireNonNull;

public class TopNApplicationResult<T>
{
    private final T handle;
    private final Long limit;
    private final OrderingScheme orderingScheme;

    public TopNApplicationResult(T handle, Long limit, OrderingScheme orderingScheme)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
    }

    public T getHandle()
    {
        return handle;
    }

    public Long getLimit()
    {
        return limit;
    }

    public OrderingScheme getOrderingScheme()
    {
        return orderingScheme;
    }
}
