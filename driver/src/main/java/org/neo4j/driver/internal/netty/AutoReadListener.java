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
package org.neo4j.driver.internal.netty;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public abstract class AutoReadListener implements GenericFutureListener<Future<Channel>>
{
    private static final AutoReadListener ENABLE = new AutoReadListener()
    {
        @Override
        protected boolean value()
        {
            return true;
        }
    };

    private static final AutoReadListener DISABLE = new AutoReadListener()
    {
        @Override
        protected boolean value()
        {
            return false;
        }
    };

    public static AutoReadListener forValue( boolean value )
    {
        return value ? ENABLE : DISABLE;
    }

    @Override
    public void operationComplete( Future<Channel> future ) throws Exception
    {
        if ( future.isSuccess() )
        {
            future.getNow().config().setAutoRead( value() );
        }
    }

    protected abstract boolean value();
}
