/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.internal.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.SelfDescribing;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;

public abstract class MatcherFactory<T> implements SelfDescribing
{
    public abstract Matcher<T> createMatcher();

    /**
     * Matches a collection based on the number of elements in the collection that match a given matcher.
     *
     * @param matcher
     *         The matcher used for counting matching elements.
     * @param count
     *         The matcher used for evaluating the number of matching elements.
     * @param <T>
     *         The type of elements in the collection.
     * @return A matcher for a collection.
     */
    public static <T> Matcher<? extends Iterable<T>> count( final Matcher<T> matcher, final Matcher<Integer> count )
    {
        return new TypeSafeDiagnosingMatcher<Iterable<T>>()
        {
            @Override
            protected boolean matchesSafely( Iterable<T> collection, Description mismatchDescription )
            {
                int matches = 0;
                for ( T item : collection )
                {
                    if ( matcher.matches( item ) )
                    {
                        matches++;
                    }
                }
                if ( count.matches( matches ) )
                {
                    return true;
                }
                mismatchDescription.appendText( "actual number of matches was " ).appendValue( matches )
                        .appendText( " in " ).appendValue( collection );
                return false;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "collection containing " )
                        .appendDescriptionOf( count )
                        .appendText( " occurences of " )
                        .appendDescriptionOf( matcher );
            }
        };
    }

    /**
     * Matches a collection that contains elements that match the specified matchers. The elements must be in the same
     * order as the given matchers, but the collection may contain other elements in between the matching elements.
     *
     * @param matchers
     *         The matchers for the elements of the collection.
     * @param <T>
     *         The type of the elements in the collection.
     * @return A matcher for a collection.
     */
    @SafeVarargs
    public static <T> Matcher<? extends Iterable<T>> containsAtLeast( final Matcher<T>... matchers )
    {
        @SuppressWarnings( "unchecked" )
        MatcherFactory<T>[] factories = new MatcherFactory[matchers.length];
        for ( int i = 0; i < factories.length; i++ )
        {
            factories[i] = matches( matchers[i] );
        }
        return containsAtLeast( factories );
    }

    @SafeVarargs
    public static <T> Matcher<? extends Iterable<T>> containsAtLeast( final MatcherFactory<T>... matcherFactories )
    {
        return new TypeSafeMatcher<Iterable<T>>()
        {
            @Override
            protected boolean matchesSafely( Iterable<T> collection )
            {
                @SuppressWarnings( "unchecked" )
                Matcher<T>[] matchers = new Matcher[matcherFactories.length];
                for ( int i = 0; i < matchers.length; i++ )
                {
                    matchers[i] = matcherFactories[i].createMatcher();
                }
                int i = 0;
                for ( T item : collection )
                {
                    if ( i >= matchers.length )
                    {
                        return true;
                    }
                    if ( matchers[i].matches( item ) )
                    {
                        i++;
                    }
                }
                return i == matchers.length;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "collection containing at least " );
                for ( int i = 0; i < matcherFactories.length; i++ )
                {
                    if ( i != 0 )
                    {
                        if ( i == matcherFactories.length - 1 )
                        {
                            description.appendText( " and " );
                        }
                        else
                        {
                            description.appendText( ", " );
                        }
                    }
                    description.appendDescriptionOf( matcherFactories[i] );
                }
                description.appendText( " (in that order) " );
            }
        };
    }

    @SafeVarargs
    public static <T> MatcherFactory<T> inAnyOrder( final Matcher<? extends T>... matchers )
    {
        return new MatcherFactory<T>()
        {
            @Override
            public Matcher<T> createMatcher()
            {
                final List<Matcher<? extends T>> remaining = new ArrayList<>( matchers.length );
                Collections.addAll( remaining, matchers );
                return new BaseMatcher<T>()
                {
                    @Override
                    public boolean matches( Object item )
                    {
                        for ( Iterator<Matcher<? extends T>> matcher = remaining.iterator(); matcher.hasNext(); )
                        {
                            if ( matcher.next().matches( item ) )
                            {
                                matcher.remove();
                                return remaining.isEmpty();
                            }
                        }
                        return remaining.isEmpty();
                    }

                    @Override
                    public void describeTo( Description description )
                    {
                        describe( description );
                    }
                };
            }

            @Override
            public void describeTo( Description description )
            {
                describe( description );
            }

            private void describe( Description description )
            {
                description.appendText( "in any order" );
                String sep = " {";
                for ( Matcher<? extends T> matcher : matchers )
                {
                    description.appendText( sep );
                    description.appendDescriptionOf( matcher );
                    sep = ", ";
                }
                description.appendText( "}" );
            }
        };
    }

    public static <T> MatcherFactory<T> matches( final Matcher<T> matcher )
    {
        return new MatcherFactory<T>()
        {
            @Override
            public Matcher<T> createMatcher()
            {
                return matcher;
            }

            @Override
            public void describeTo( Description description )
            {
                matcher.describeTo( description );
            }
        };
    }
}
