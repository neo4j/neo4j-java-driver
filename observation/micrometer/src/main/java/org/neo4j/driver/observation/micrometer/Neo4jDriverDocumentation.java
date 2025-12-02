/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.observation.micrometer;

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.ObservationDocumentation;
import java.util.Map;
import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.property_encryption.EncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.EncapsulatedKeyRecordRepository;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import org.neo4j.driver.property_encryption.PropertyDecryptionRequest;
import org.neo4j.driver.property_encryption.PropertyEncryption;
import org.neo4j.driver.property_encryption.PropertyEncryptionRequest;
import org.neo4j.driver.reactivestreams.ReactiveResult;

enum Neo4jDriverDocumentation implements ObservationDocumentation {
    /**
     * Observes {@link Session#run(Query)} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative session types.
     */
    SESSION_RUN {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultSessionRunConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return SessionRunLowCardinalityKeyNames.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return SessionRunHighCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link Session#executeWrite(TransactionCallback)} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative session types.
     */
    SESSION_EXECUTE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultSessionExecuteConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return SessionExecuteLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link Session#close()} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative session types.
     */
    SESSION_CLOSE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultSessionCloseConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return SessionCloseLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link Session#beginTransaction()} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative session types.
     */
    TRANSACTION_BEGIN {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultTransactionBeginConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return TransactionBeginLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link Transaction#run(String)} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative transaction types.
     */
    TRANSACTION_RUN {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultTransactionRunConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return TransactionRunLowCardinalityKeyNames.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return TransactionRunHighCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link Transaction#commit()} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative transaction types.
     */
    TRANSACTION_COMMIT {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultTransactionCommitConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return TransactionCommitLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link Transaction#rollback()} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative transaction types.
     */
    TRANSACTION_ROLLBACK {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultTransactionRollbackConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return TransactionRollbackLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link Transaction#close()} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative transaction types.
     */
    TRANSACTION_CLOSE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultTransactionCloseConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return TransactionCloseLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link Result#peek()} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative result types.
     * <p>
     * Note that only those executions that require network exchange are observed.
     */
    RESULT_PEEK {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultResultPeekConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return ResultPeekLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link Result#next()} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative result types.
     * <p>
     * Note that only those executions that require network exchange are observed.
     */
    RESULT_NEXT {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultResultNextConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return ResultNextLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link Result#single()} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative result types.
     * <p>
     * Note that only those executions that require network exchange are observed.
     */
    RESULT_SINGLE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultResultSingleConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return ResultSingleLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link Result#list()} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative result types.
     * <p>
     * Note that only those executions that require network exchange are observed.
     */
    RESULT_LIST {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultResultListConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return ResultListLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link ReactiveResult#records()} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative result types.
     * <p>
     * Note that only those executions that require network exchange are observed.
     */
    RESULT_RECORDS {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultResultRecordsConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return ReactiveRecordsLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link Result#consume()} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative result types.
     * <p>
     * Note that only those executions that require network exchange are observed.
     */
    RESULT_CONSUME {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultResultConsumeConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return ResultConsumeLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link PropertyEncryption#encryptToBytes(PropertyEncryptionRequest)} execution.
     * <p>
     * This also applies to the alternative property encryption types.
     */
    PROPERTY_ENCRYPTION_ENCRYPT_TO_BYTES {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultEncryptToBytesConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return EncryptToBytesLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link PropertyEncryption#decrypt(PropertyDecryptionRequest)} execution.
     * <p>
     * This also applies to the alternative property encryption types.
     */
    PROPERTY_ENCRYPTION_DECRYPT {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultDecryptConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return DecryptLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link EncapsulatedKeyManager#create(String, KeyEncapsulationOptions)} execution.
     * <p>
     * This also applies to the other variants of this method, including those of the alternative encapsulated key
     * manager types.
     */
    CREATE_ENCAPSULATED_KEY {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultCreateEncapsulatedKeyConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return CreateEncapsulatedKeyLowCardinalityKeyNames.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return CreateEncapsulatedKeyHighCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link EncapsulatedKeyManager#findByAlias(String)} execution.
     * <p>
     * This also applies to the alternative encapsulated key manager types.
     */
    FIND_ENCAPSULATED_KEY {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultFindEncapsulatedKeyByAliasConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return FindEncapsulatedKeyByAliasLowCardinalityKeyNames.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return FindEncapsulatedKeyByAliasHighCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link EncapsulatedKeyManager#setAliasById(String, String)} execution.
     * <p>
     * This also applies to the alternative encapsulated key manager types.
     */
    SET_ENCAPSULATED_KEY_ALIAS {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultSetEncapsulatedKeyAliasConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return SetEncapsulatedKeyAliasLowCardinalityKeyNames.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return SetEncapsulatedKeyAliasHighCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link EncapsulatedKeyManager#findByAlias(String)} execution.
     * <p>
     * This also applies to the alternative encapsulated key manager types.
     */
    DELETE_ENCAPSULATED_KEY {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultDeleteEncapsulatedKeyConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return DeleteEncapsulatedKeyLowCardinalityKeyNames.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return DeleteEncapsulatedKeyHighCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link KeyEncapsulationService#encapsulate(KeyEncapsulationOptions)} execution.
     */
    KEY_ENCAPSULATION_SERVICE_ENCAPSULATE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultKeyEncapsulationServiceEncapsulateConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return KeyEncapsulationServiceEncapsulateLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link KeyEncapsulationService#decapsulate(byte[], Map)} execution.
     */
    KEY_ENCAPSULATION_SERVICE_DECAPSULATE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultKeyEncapsulationServiceDecapsulateConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return KeyEncapsulationServiceDecapsulateLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link EncapsulatedKeyRecordRepository#findById(String)} execution.
     */
    KEY_REPOSITORY_FIND_BY_ID {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultEncapsulatedKeyRepositoryFindByIdConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return EncapsulatedKeyRepositoryFindByIdLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link EncapsulatedKeyRecordRepository#findByAlias(String)} execution.
     */
    KEY_REPOSITORY_FIND_BY_ALIAS {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultEncapsulatedKeyRepositoryFindByAliasConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return EncapsulatedKeyRepositoryFindByAliasLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link EncapsulatedKeyRecordRepository#create(String, byte[], Map)} execution.
     */
    KEY_REPOSITORY_CREATE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultEncapsulatedKeyRepositoryCreateConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return EncapsulatedKeyRepositoryCreateLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link EncapsulatedKeyRecordRepository#setAliasById(String, String)} execution.
     */
    KEY_REPOSITORY_FIND_SET_ALIAS_BY_ID {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultEncapsulatedKeyRepositorySetAliasByIdConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return EncapsulatedKeyRepositorySetAliasByIdLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes {@link EncapsulatedKeyRecordRepository#deleteById(String)} execution.
     */
    KEY_REPOSITORY_FIND_DELETE_BY_ID {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultEncapsulatedKeyRepositoryDeleteByIdConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return EncapsulatedKeyRepositoryDeleteByIdLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes a new connection pool creation.
     */
    CONNECTION_POOL_CREATE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultConnectionPoolCreateConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return ConnectionPoolCreateLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes connection pool closure.
     */
    CONNECTION_POOL_CLOSE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultConnectionPoolCloseConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return ConnectionPoolCloseLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes a new connection creation by connection pool.
     */
    POOLED_CONNECTION_CREATE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultPooledConnectionCreateConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return PooledConnectionCreateLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes connection acquisition from connection pool.
     */
    POOLED_CONNECTION_PENDING {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultPooledConnectionAcquireConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return PooledConnectionAcquireLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes connection usage.
     * <p>
     * It starts when connection is acquired from connection pool and is stopped before the connection is made available
     * for re-use or deleted.
     */
    POOLED_CONNECTION_IN_USE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultPooledConnectionInUseConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return PooledConnectionInUseLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes connection closure.
     */
    POOLED_CONNECTION_CLOSE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultPooledConnectionCloseConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return PooledConnectionCloseLowCardinalityKeyNames.values();
        }
    },
    /**
     * Observes Bolt messages handling.
     * <p>
     * The Neo4j Java Driver uses Neo4j Bolt Connection to exchange Bolt messages with the server. This
     * observation works on this integration level. It starts when messages are submitted to Bolt Connection and stops
     * when the exchange is finished. This allows observations to happen from driver perspective, but it does not focus
     * on the lowest implementation levels.
     */
    BOLT_HANDLE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultBoltHandleConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return BoltHandleLowCardinalityKeyNames.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return BoltHandleHighCardinalityKeyNames.values();
        }
    },
    /**
     * Observes Bolt messages exchange.
     * <p>
     * This observation is handled on Neo4j Bolt Connection level and it focuses on lower level of Bolt messages
     * exchange.
     */
    BOLT_EXCHANGE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultBoltExchangeConvention.class;
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return BoltExchangeHighCardinalityKeyNames.values();
        }
    },
    /**
     * Observes HTTP exchange.
     * <p>
     * This observation focuses on HTTP exchanges that would typically be used when the driver uses HTTP scheme instead
     * of Bolt.
     */
    HTTP_EXCHANGE {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return DefaultHttpExchangeConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return HttpExchangeLowCardinalityKeyNames.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return HttpExchangeHighCardinalityKeyNames.values();
        }
    };

    enum SessionRunLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The session type.
         */
        SESSION_TYPE {
            @Override
            public String asString() {
                return "neo4j.session.type";
            }
        }
    }

    enum SessionRunHighCardinalityKeyNames implements KeyName {
        /**
         * The query text. It is included if the query has parameters or when explicitly enabled in the provider.
         */
        DB_QUERY_TEXT {
            @Override
            public String asString() {
                return "db.query.text";
            }

            @Override
            public boolean isRequired() {
                return false;
            }
        },
        /**
         * The query parameters. The parameters are included only when explicitly enabled in the provider.
         */
        DB_QUERY_PARAMETER_FORMAT {
            @Override
            public String asString() {
                return "db.query.parameter.<key>";
            }

            @Override
            public boolean isRequired() {
                return false;
            }
        }
    }

    enum SessionExecuteLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The session type.
         */
        SESSION_TYPE {
            @Override
            public String asString() {
                return "neo4j.session.type";
            }
        },
        /**
         * The access mode.
         */
        SESSION_MODE {
            @Override
            public String asString() {
                return "neo4j.session.mode";
            }
        }
    }

    enum SessionCloseLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The session type.
         */
        SESSION_TYPE {
            @Override
            public String asString() {
                return "neo4j.session.type";
            }
        }
    }

    enum TransactionBeginLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The transaction type.
         */
        TRANSACTION_TYPE {
            @Override
            public String asString() {
                return "neo4j.transaction.type";
            }
        }
    }

    enum TransactionRunLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The transaction type.
         */
        TRANSACTION_TYPE {
            @Override
            public String asString() {
                return "neo4j.transaction.type";
            }
        }
    }

    enum TransactionRunHighCardinalityKeyNames implements KeyName {
        /**
         * The query text. It is included if the query has parameters or when explicitly enabled in the provider.
         */
        DB_QUERY_TEXT {
            @Override
            public String asString() {
                return "db.query.text";
            }

            @Override
            public boolean isRequired() {
                return false;
            }
        },
        /**
         * The query parameters. The parameters are included only when explicitly enabled in the provider.
         */
        DB_QUERY_PARAMETER_FORMAT {
            @Override
            public String asString() {
                return "db.query.parameter.<key>";
            }

            @Override
            public boolean isRequired() {
                return false;
            }
        }
    }

    enum TransactionCommitLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The transaction type.
         */
        TRANSACTION_TYPE {
            @Override
            public String asString() {
                return "neo4j.transaction.type";
            }
        }
    }

    enum TransactionRollbackLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The transaction type.
         */
        TRANSACTION_TYPE {
            @Override
            public String asString() {
                return "neo4j.transaction.type";
            }
        }
    }

    enum TransactionCloseLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The transaction type.
         */
        TRANSACTION_TYPE {
            @Override
            public String asString() {
                return "neo4j.transaction.type";
            }
        }
    }

    enum ResultPeekLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The result type.
         */
        RESULT_TYPE {
            @Override
            public String asString() {
                return "neo4j.result.type";
            }
        }
    }

    enum ResultNextLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The result type.
         */
        RESULT_TYPE {
            @Override
            public String asString() {
                return "neo4j.result.type";
            }
        }
    }

    enum ResultSingleLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The result type.
         */
        RESULT_TYPE {
            @Override
            public String asString() {
                return "neo4j.result.type";
            }
        }
    }

    enum ResultListLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The result type.
         */
        RESULT_TYPE {
            @Override
            public String asString() {
                return "neo4j.result.type";
            }
        }
    }

    enum ResultConsumeLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The result type.
         */
        RESULT_TYPE {
            @Override
            public String asString() {
                return "neo4j.result.type";
            }
        }
    }

    enum ReactiveRecordsLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The result type.
         */
        RESULT_TYPE {
            @Override
            public String asString() {
                return "neo4j.result.type";
            }
        }
    }

    enum EncryptToBytesLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The property encryption type.
         */
        PROPERTY_ENCRYPTION_TYPE {
            @Override
            public String asString() {
                return "neo4j.property.encryption.type";
            }
        }
    }

    enum DecryptLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The property encryption type.
         */
        PROPERTY_ENCRYPTION_TYPE {
            @Override
            public String asString() {
                return "neo4j.property.encryption.type";
            }
        }
    }

    enum CreateEncapsulatedKeyLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The encapsulated key manager type.
         */
        ENCAPSULATED_KEY_MANAGER_TYPE {
            @Override
            public String asString() {
                return "neo4j.property.encryption.encapsulated.key.manager.type";
            }
        }
    }

    enum CreateEncapsulatedKeyHighCardinalityKeyNames implements KeyName {
        /**
         * The key alias if available.
         */
        KEY_ALIAS {
            @Override
            public String asString() {
                return "neo4j.property.encryption.encapsulated.key.alias";
            }

            @Override
            public boolean isRequired() {
                return false;
            }
        }
    }

    enum FindEncapsulatedKeyByAliasLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The encapsulated key manager type.
         */
        ENCAPSULATED_KEY_MANAGER_TYPE {
            @Override
            public String asString() {
                return "neo4j.property.encryption.encapsulated.key.manager.type";
            }
        }
    }

    enum FindEncapsulatedKeyByAliasHighCardinalityKeyNames implements KeyName {
        /**
         * The key alias if available.
         */
        KEY_ALIAS {
            @Override
            public String asString() {
                return "neo4j.property.encryption.encapsulated.key.alias";
            }

            @Override
            public boolean isRequired() {
                return false;
            }
        }
    }

    enum DeleteEncapsulatedKeyLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The encapsulated key manager type.
         */
        ENCAPSULATED_KEY_MANAGER_TYPE {
            @Override
            public String asString() {
                return "neo4j.property.encryption.encapsulated.key.manager.type";
            }
        }
    }

    enum DeleteEncapsulatedKeyHighCardinalityKeyNames implements KeyName {
        /**
         * The key id.
         */
        KEY_ID {
            @Override
            public String asString() {
                return "neo4j.property.encryption.encapsulated.key.id";
            }
        }
    }

    enum SetEncapsulatedKeyAliasLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The encapsulated key manager type.
         */
        ENCAPSULATED_KEY_MANAGER_TYPE {
            @Override
            public String asString() {
                return "neo4j.property.encryption.encapsulated.key.manager.type";
            }
        }
    }

    enum SetEncapsulatedKeyAliasHighCardinalityKeyNames implements KeyName {
        /**
         * The key id.
         */
        KEY_ID {
            @Override
            public String asString() {
                return "neo4j.property.encryption.encapsulated.key.id";
            }
        },
        /**
         * The key alias if available.
         */
        KEY_ALIAS {
            @Override
            public String asString() {
                return "neo4j.property.encryption.encapsulated.key.alias";
            }

            @Override
            public boolean isRequired() {
                return false;
            }
        }
    }

    enum KeyEncapsulationServiceEncapsulateLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        }
    }

    enum KeyEncapsulationServiceDecapsulateLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        }
    }

    enum EncapsulatedKeyRepositoryFindByIdLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        }
    }

    enum EncapsulatedKeyRepositoryFindByAliasLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        }
    }

    enum EncapsulatedKeyRepositoryCreateLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        }
    }

    enum EncapsulatedKeyRepositorySetAliasByIdLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        }
    }

    enum EncapsulatedKeyRepositoryDeleteByIdLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        }
    }

    enum ConnectionPoolCloseLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },

        /**
         * The pool name in the following format: <b>host[:port]-id</b>. Note that the port part is optional if default scheme port is used.
         */
        POOL_NAME {
            @Override
            public String asString() {
                return "db.client.connection.pool.name";
            }
        }
    }

    enum PooledConnectionAcquireLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },

        /**
         * The pool name in the following format: <b>host[:port]-id</b>. Note that the port part is optional if default scheme port is used.
         */
        POOL_NAME {
            @Override
            public String asString() {
                return "db.client.connection.pool.name";
            }
        }
    }

    enum PooledConnectionCloseLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },

        /**
         * The pool name in the following format: <b>host[:port]-id</b>. Note that the port part is optional if default scheme port is used.
         */
        POOL_NAME {
            @Override
            public String asString() {
                return "db.client.connection.pool.name";
            }
        }
    }

    enum PooledConnectionCreateLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },

        /**
         * The pool name in the following format: <b>host[:port]-id</b>. Note that the port part is optional if default scheme port is used.
         */
        POOL_NAME {
            @Override
            public String asString() {
                return "db.client.connection.pool.name";
            }
        }
    }

    enum PooledConnectionInUseLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },

        /**
         * The pool name in the following format: <b>host[:port]-id</b>. Note that the port part is optional if default scheme port is used.
         */
        POOL_NAME {
            @Override
            public String asString() {
                return "db.client.connection.pool.name";
            }
        }
    }

    enum ConnectionPoolCreateLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },

        /**
         * The pool name in the following format: <b>host[:port]-id</b>. Note that the port part is optional if default scheme port is used.
         */
        POOL_NAME {
            @Override
            public String asString() {
                return "db.client.connection.pool.name";
            }
        }
    }

    enum BoltHandleLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        }
    }

    enum BoltHandleHighCardinalityKeyNames implements KeyName {
        /**
         * The message names.
         */
        MESSAGES {
            @Override
            public String asString() {
                return "neo4j.bolt.messages";
            }
        }
    }

    enum BoltExchangeLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The network protocol name. It is always <b>bolt</b>.
         */
        NETWORK_PROTOCOL_NAME {
            @Override
            public String asString() {
                return "network.protocol.name";
            }
        },
        /**
         * The network protocol version. It always represents Bolt version.
         */
        NETWORK_PROTOCOL_VERSION {
            @Override
            public String asString() {
                return "network.protocol.version";
            }
        },
        /**
         * The server address.
         */
        SERVER_ADDRESS {
            @Override
            public String asString() {
                return "server.address";
            }
        },
        /**
         * The server port.
         */
        SERVER_PORT {
            @Override
            public String asString() {
                return "server.port";
            }
        }
    }

    enum BoltExchangeHighCardinalityKeyNames implements KeyName {
        /**
         * The message names.
         */
        MESSAGES {
            @Override
            public String asString() {
                return "neo4j.bolt.messages";
            }
        }
    }

    enum HttpExchangeLowCardinalityKeyNames implements KeyName {
        /**
         * The DBMS product name. It is always <b>neo4j</b>.
         */
        DB_SYSTEM_NAME {
            @Override
            public String asString() {
                return "db.system.name";
            }
        },
        /**
         * The HTTP request method.
         */
        HTTP_REQUEST_METHOD {
            @Override
            public String asString() {
                return "http.request.method";
            }
        },
        /**
         * The URL scheme. It is included only when explicitly enabled in the provider.
         */
        URL_SCHEME {
            @Override
            public String asString() {
                return "url.scheme";
            }

            @Override
            public boolean isRequired() {
                return false;
            }
        },
        /**
         * The network protocol name. It is always <b>http</b>.
         */
        NETWORK_PROTOCOL_NAME {
            @Override
            public String asString() {
                return "network.protocol.name";
            }
        },
        /**
         * The network protocol version. It always represents HTTP version.
         */
        NETWORK_PROTOCOL_VERSION {
            @Override
            public String asString() {
                return "network.protocol.version";
            }
        },
        /**
         * The server address.
         */
        SERVER_ADDRESS {
            @Override
            public String asString() {
                return "server.address";
            }
        },
        /**
         * The server port.
         */
        SERVER_PORT {
            @Override
            public String asString() {
                return "server.port";
            }
        },
        /**
         * The URI template. It is included only when explicitly enabled in the provider.
         */
        URL_TEMPLATE {
            @Override
            public String asString() {
                return "url.template";
            }

            @Override
            public boolean isRequired() {
                return false;
            }
        },
        /**
         * The HTTP response status code.
         */
        HTTP_RESPONSE_STATUS_CODE {
            @Override
            public String asString() {
                return "http.response.status.code";
            }
        },
        /**
         * The error type.
         */
        ERROR_TYPE {
            @Override
            public String asString() {
                return "error.type";
            }
        }
    }

    enum HttpExchangeHighCardinalityKeyNames implements KeyName {
        /**
         * The full URL.
         */
        URL_FULL {
            @Override
            public String asString() {
                return "url.full";
            }
        },
        /**
         * The HTTP request headers. These are included only when explicitly enabled in the provider.
         */
        HTTP_REQUEST_HEADER_FORMAT {
            @Override
            public String asString() {
                return "http.request.header.<key>";
            }

            @Override
            public boolean isRequired() {
                return false;
            }
        },
        /**
         * The HTTP response headers. These are included only when explicitly enabled in the provider.
         */
        HTTP_RESPONSE_HEADER_FORMAT {
            @Override
            public String asString() {
                return "http.response.header.<key>";
            }

            @Override
            public boolean isRequired() {
                return false;
            }
        }
    }
}
