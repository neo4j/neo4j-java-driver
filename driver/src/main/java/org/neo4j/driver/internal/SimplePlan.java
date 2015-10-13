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
package org.neo4j.driver.internal;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.driver.Plan;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

import static java.lang.String.format;

public class SimplePlan implements Plan
{
    private final String operatorType;
    private final List<String> identifiers;
    private final Map<String, Value> arguments;
    private final List<? extends Plan> children;

    // Only call when sub-classing, for constructing plans, use .plan instead
    protected SimplePlan(
            String operatorType,
            Map<String, Value> arguments,
            List<String> identifiers,
            List<? extends Plan> children )
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
    public List<? extends Plan> children()
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

        SimplePlan that = (SimplePlan) o;

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

    public static SimplePlan plan(
            String operatorType,
            Map<String, Value> arguments,
            List<String> identifiers,
            List<? extends Plan> children )
    {
        return new SimplePlan( operatorType, arguments, identifiers, children );
    }

    public static final Function<Value, Plan> FROM_VALUE = new Converter();

    static class Converter implements Function<Value, Plan>
    {
        @Override
        public Plan apply( Value plan )
        {
            final String operatorType = plan.get( "operatorType" ).javaString();

            final Value argumentsValue = plan.get( "args" );
            final Map<String, Value> arguments = argumentsValue == null
                    ? Collections.<String, Value>emptyMap()
                    : argumentsValue.javaMap( Values.valueAsIs() );

            final Value identifiersValue = plan.get( "identifiers" );
            final List<String> identifiers = identifiersValue == null
                    ? Collections.<String>emptyList()
                    : identifiersValue.javaList( Values.valueToString() );

            final Value childrenValue = plan.get( "children" );
            final List<Plan> children = childrenValue == null
                    ? Collections.<Plan>emptyList()
                    : childrenValue.javaList( this );

            return plan( operatorType, arguments, identifiers, children );
        }
    }
}

