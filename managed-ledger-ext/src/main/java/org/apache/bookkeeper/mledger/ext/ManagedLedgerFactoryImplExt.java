/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.ext;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.MetaStore;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extended factory that creates ManagedLedgerImplExt instances.
 * This factory overrides the createManagedLedgerInstance method to provide
 * managed ledgers with extended trimming capabilities.
 */
public class ManagedLedgerFactoryImplExt extends ManagedLedgerFactoryImpl {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerFactoryImplExt.class);

    public ManagedLedgerFactoryImplExt(MetadataStoreExtended metadataStore,
            BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
            ManagedLedgerFactoryConfig config, StatsLogger statsLogger) throws Exception {
        super(metadataStore, bookKeeperGroupFactory, config, statsLogger);
    }

    @Override
    protected ManagedLedgerImpl createManagedLedgerInstance(BookKeeper bk, MetaStore store,
            ManagedLedgerConfig config, OrderedScheduler scheduledExecutor,
            String name, Supplier<CompletableFuture<Boolean>> mlOwnershipChecker) {
        // Create ManagedLedgerImplExt instead of ManagedLedgerImpl for extended trimming capabilities
        if (config.getShadowSource() == null) {
            log.debug("[{}] Creating ManagedLedgerImplExt with extended trimming capabilities", name);
            return new ManagedLedgerImplExt(this, bk, store, config, scheduledExecutor, name, mlOwnershipChecker);
        } else {
            // For shadow managed ledgers, fall back to the default implementation
            log.debug("[{}] Using default ShadowManagedLedgerImpl for shadow source", name);
            return super.createManagedLedgerInstance(bk, store, config, scheduledExecutor, name, mlOwnershipChecker);
        }
    }
}
