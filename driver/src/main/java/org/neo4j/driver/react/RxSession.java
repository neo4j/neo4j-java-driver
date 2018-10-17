/*
 * Copyright (c) 2002-2009 "Neo4j,"
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
package org.neo4j.driver.react;

import org.reactivestreams.Publisher;

import java.util.Map;

import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.TransactionConfig;


public interface RxSession extends RxStatementRunner
{
    RxTransaction beginTransaction();

    RxTransaction beginTransaction( TransactionConfig config );

    <T> Publisher<T> readTransaction( RxTransactionWork<Publisher<T>> work );

    <T> Publisher<T> readTransaction( RxTransactionWork<Publisher<T>> work, TransactionConfig config );

    <T> Publisher<T> writeTransaction( RxTransactionWork<Publisher<T>> work );

    <T> Publisher<T> writeTransaction( RxTransactionWork<Publisher<T>> work, TransactionConfig config );
    
    RxResult run( String statement, TransactionConfig config );

    RxResult run( String statement, Map<String,Object> parameters, TransactionConfig config );

    RxResult run( Statement statement, TransactionConfig config );

    String lastBookmark();

    @Deprecated
    void reset();

    Publisher<Void> close();
}
