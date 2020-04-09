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


import io.prestosql.spi.Symbol;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class AggregationApplicationResult<T>
{
    private final T handle;
    private final Map<Symbol, ColumnHandle> assignments;

    public AggregationApplicationResult(T handle, Map<Symbol, ColumnHandle> assignments)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.assignments = requireNonNull(assignments, "assignments is null");
    }

    public T getHandle()
    {
        return handle;
    }

    public Map<Symbol, ColumnHandle> getAssignments() {
        return assignments;
    }
}
