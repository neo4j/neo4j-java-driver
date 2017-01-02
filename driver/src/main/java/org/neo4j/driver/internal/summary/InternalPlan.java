/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.summary;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.summary.Plan;
import org.neo4j.driver.v1.util.Function;

import static java.lang.String.format;
import static org.neo4j.driver.v1.Values.ofString;

public class InternalPlan<T extends Plan> implements Plan
{
    private final String operatorType;
    private final List<String> identifiers;
    private final Map<String, Value> arguments;
    private final List<T> children;

    // Only call when sub-classing, for constructing plans, use .plan instead
    protected InternalPlan(
            String operatorType,
            Map<String, Value> arguments,
            List<String> identifiers,
            List<T> children )
    {
        this.operatorType = operatorType;
        this.identifiers = identifiers;
        this.arguments = arguments;
        this.children = children;
    }

    @Override
    public String operatorType()
    {
        return operatorType;
    }

    @Override
    public List<String> identifiers()
    {
        return identifiers;
    }

    @Override
    public Map<String, Value> arguments()
    {
        return arguments;
    }

    @Override
    public List<T> children()
    {
        return children;
    }

    @Override
    public String toString()
    {
        return format(
            "SimplePlanTreeNode{operatorType='%s', arguments=%s, identifiers=%s, children=%s}",
            operatorType, arguments, identifiers, children
        );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        InternalPlan that = (InternalPlan) o;

        return operatorType.equals( that.operatorType )
            && arguments.equals( that.arguments )
            && identifiers.equals( that.identifiers )
            && children.equals( that.children );
    }

    @Override
    public int hashCode()
    {
        int result = operatorType.hashCode();
        result = 31 * result + identifiers.hashCode();
        result = 31 * result + arguments.hashCode();
        result = 31 * result + children.hashCode();
        return result;
    }

    public static Plan plan(
            String operatorType,
            Map<String, Value> arguments,
            List<String> identifiers,
            List<Plan> children )
    {
        return EXPLAIN_PLAN.create( operatorType, arguments, identifiers, children, null );
    }

    public static final PlanCreator<Plan> EXPLAIN_PLAN = new PlanCreator<Plan>()
    {
        @Override
        public Plan create( String operatorType, Map<String,Value> arguments, List<String> identifiers, List<Plan> children, Value originalPlanValue )
        {
            return new InternalPlan<>( operatorType, arguments, identifiers, children );
        }
    };

    /** Builds a regular plan without profiling information - eg. a plan that came as a result of an `EXPLAIN` statement */
    public static final Function<Value, Plan> EXPLAIN_PLAN_FROM_VALUE = new Converter<>(EXPLAIN_PLAN);

    /**
     * Since a plan with or without profiling looks almost the same, we just keep two impls. of this
     * around to contain the small difference, and share the rest of the code for building plan trees.
     * @param <T>
     */
    interface PlanCreator<T extends Plan>
    {
        T create( String operatorType,
                  Map<String, Value> arguments,
                  List<String> identifiers,
                  List<T> children,
                  Value originalPlanValue );
    }

    static class Converter<T extends Plan> implements Function<Value, T>
    {
        private final PlanCreator<T> planCreator;

        public Converter( PlanCreator<T> planCreator )
        {
            this.planCreator = planCreator;
        }

        @Override
        public T apply( Value plan )
        {
            final String operatorType = plan.get( "operatorType" ).asString();

            final Value argumentsValue = plan.get( "args" );
            final Map<String, Value> arguments = argumentsValue.isNull()
                    ? Collections.<String, Value>emptyMap()
                    : argumentsValue.asMap( Values.ofValue() );

            final Value identifiersValue = plan.get( "identifiers" );
            final List<String> identifiers = identifiersValue.isNull()
                    ? Collections.<String>emptyList()
                    : identifiersValue.asList( ofString() );

            final Value childrenValue = plan.get( "children" );
            final List<T> children = childrenValue.isNull()
                    ? Collections.<T>emptyList()
                    : childrenValue.asList( this );

            return planCreator.create( operatorType, arguments, identifiers, children, plan );
        }
    }
}

