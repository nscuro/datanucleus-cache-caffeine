/*
 * This file is part of DataNucleus Cache Caffeine.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) Niklas Düster. All Rights Reserved.
 */
package io.github.nscuro.datanucleus.cache.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.datanucleus.Configuration;
import org.datanucleus.NucleusContext;
import org.datanucleus.cache.AbstractLevel2Cache;
import org.datanucleus.cache.CachedPC;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static io.github.nscuro.datanucleus.cache.caffeine.CaffeineCachePropertyNames.PROPERTY_CACHE_L2_CAFFEINE_INITIAL_CAPACITY;
import static org.datanucleus.PropertyNames.PROPERTY_CACHE_L2_EXPIRY_MILLIS;
import static org.datanucleus.PropertyNames.PROPERTY_CACHE_L2_MAXSIZE;
import static org.datanucleus.PropertyNames.PROPERTY_CACHE_L2_STATISTICS_ENABLED;

public class CaffeineLevel2Cache extends AbstractLevel2Cache {

    private final Cache<Object, Object> caffeineCache;

    public CaffeineLevel2Cache(NucleusContext nucleusCtx) {
        super(nucleusCtx);

        final Configuration config = nucleusCtx.getConfiguration();

        final Caffeine<Object, Object> caffeine = Caffeine.newBuilder();
        if (config.getIntProperty(PROPERTY_CACHE_L2_MAXSIZE) >= 0) {
            caffeine.maximumSize(config.getIntProperty(PROPERTY_CACHE_L2_MAXSIZE));
        }
        if (config.getIntProperty(PROPERTY_CACHE_L2_CAFFEINE_INITIAL_CAPACITY) > 0) {
            caffeine.initialCapacity(config.getIntProperty(PROPERTY_CACHE_L2_CAFFEINE_INITIAL_CAPACITY));
        }
        if (config.getIntProperty(PROPERTY_CACHE_L2_EXPIRY_MILLIS) > 0) {
            caffeine.expireAfterWrite(config.getIntProperty(PROPERTY_CACHE_L2_EXPIRY_MILLIS), TimeUnit.MILLISECONDS);
        }
        if (config.getBooleanProperty(PROPERTY_CACHE_L2_STATISTICS_ENABLED)) {
            caffeine.recordStats();
        }

        caffeineCache = caffeine.build();
    }

    public Cache<?, ?> getCaffeineCache() {
        return caffeineCache;
    }

    @Override
    public void close() {
        // Nothing to do.
    }

    @Override
    public void evict(Object oid) {
        caffeineCache.invalidate(oid);
    }

    @Override
    public void evictAll() {
        caffeineCache.invalidateAll();
    }

    @Override
    public void evictAll(Object[] oids) {
        caffeineCache.invalidateAll(Arrays.asList(oids));
    }

    @Override
    public void evictAll(Collection oids) {
        caffeineCache.invalidateAll(oids);
    }

    @Override
    public void evictAll(Class pcClass, boolean subclasses) {
        // Not supported.
    }

    @Override
    public CachedPC get(Object oid) {
        return (CachedPC) caffeineCache.getIfPresent(oid);
    }

    @Override
    public CachedPC put(Object oid, CachedPC pc) {
        if (oid == null || pc == null) {
            return null;
        }

        caffeineCache.put(oid, pc);
        return pc;
    }

    @Override
    public boolean containsOid(Object oid) {
        return caffeineCache.getIfPresent(oid) != null;
    }

    @Override
    public int getSize() {
        // Caffeine does not guarantee accuracy of the estimated size,
        // because invalidation due to expiry is performed ad-hoc during
        // writes (and occasionally reads). We don't expect #getSize() to
        // be called often during normal operation, so we take the potential
        // performance penalty of performing cleanUp here, in favor of more
        // accurate size estimates.
        caffeineCache.cleanUp();

        return Math.toIntExact(caffeineCache.estimatedSize());
    }
}
