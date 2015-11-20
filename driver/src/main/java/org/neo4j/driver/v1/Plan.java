/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
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
package org.neo4j.driver.v1;

import java.util.List;
import java.util.Map;

/**
 * This describes the plan that the database planner produced and used (or will use) to execute your statement.
 * This can be extremely helpful in understanding what a statement is doing, and how to optimize it. For more
 * details, see the Neo4j Manual.
 *
 * The plan for the statement is a tree of plans - each sub-tree containing zero or more child plans. The statement
 * starts with the root plan. Each sub-plan is of a specific {@link #operatorType() operator type}, which describes
 * what that part of the plan does - for instance, perform an index lookup or filter results. The Neo4j Manual contains
 * a reference of the available operator types, and these may differ across Neo4j versions.
 *
 * For a simple view of a plan, the {@code toString} method will give a human-readable rendering of the tree.
 */
public interface Plan
{
    /**
     * @return the operation this plan is performing.
     */
    String operatorType();

    /**
     * Many {@link #operatorType() operators} have arguments defining their specific behavior. This map contains
     * those arguments.
     *
     * @return the arguments for the {@link #operatorType() operator} used.
     */
    Map<String,Value> arguments();

    /**
     * Identifiers used by this part of the plan. These can be both identifiers introduce by you, or automatically
     * generated identifiers.
     * @return a list of identifiers used by this plan.
     */
    List<String> identifiers();

    /**
     * As noted in the class-level javadoc, a plan is a tree, where each child is another plan. The children are where
     * this part of the plan gets its input records - unless this is an {@link #operatorType() operator} that introduces
     * new records on its own.
     * @return zero or more child plans.
     */
    List<? extends Plan> children();
}
