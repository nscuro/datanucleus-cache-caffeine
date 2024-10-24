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
import org.datanucleus.cache.CacheUniqueKey;
import org.datanucleus.cache.CachedPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import static io.github.nscuro.datanucleus.cache.caffeine.CaffeineCachePropertyNames.PROPERTY_CACHE_L2_CAFFEINE_EXPIRY_MODE;
import static io.github.nscuro.datanucleus.cache.caffeine.CaffeineCachePropertyNames.PROPERTY_CACHE_L2_CAFFEINE_INITIAL_CAPACITY;
import static org.datanucleus.PropertyNames.PROPERTY_CACHE_L2_EXPIRY_MILLIS;
import static org.datanucleus.PropertyNames.PROPERTY_CACHE_L2_MAXSIZE;
import static org.datanucleus.PropertyNames.PROPERTY_CACHE_L2_STATISTICS_ENABLED;

public class CaffeineLevel2Cache extends AbstractLevel2Cache {

    private static final Logger LOGGER = LoggerFactory.getLogger(CaffeineLevel2Cache.class);
    private static final String EXPIRY_MODE_AFTER_ACCESS = "after-access";
    private static final String EXPIRY_MODE_AFTER_WRITE = "after-write";

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
            final Duration expiryDuration = Duration.ofMillis(config.getIntProperty(PROPERTY_CACHE_L2_EXPIRY_MILLIS));

            if (EXPIRY_MODE_AFTER_ACCESS.equalsIgnoreCase(config.getStringProperty(PROPERTY_CACHE_L2_CAFFEINE_EXPIRY_MODE))) {
                caffeine.expireAfterAccess(expiryDuration);
            } else if (EXPIRY_MODE_AFTER_WRITE.equalsIgnoreCase(config.getStringProperty(PROPERTY_CACHE_L2_CAFFEINE_EXPIRY_MODE))) {
                caffeine.expireAfterWrite(expiryDuration);
            } else {
                LOGGER.warn("No expiry mode ({}) configured, assuming {}",
                        PROPERTY_CACHE_L2_CAFFEINE_EXPIRY_MODE, EXPIRY_MODE_AFTER_ACCESS);
                caffeine.expireAfterAccess(expiryDuration);
            }
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
        evictAll();
    }

    @Override
    public void evict(Object oid) {
        caffeineCache.invalidate(oid);
    }

    @Override
    public void removeUnique(CacheUniqueKey key) {
        evict(key);
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
    public CachedPC getUnique(CacheUniqueKey key) {
        return get(key);
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
    public CachedPC putUnique(CacheUniqueKey key, CachedPC pc) {
        return put(key, pc);
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
