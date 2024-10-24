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

import io.github.nscuro.datanucleus.cache.caffeine.test.model.Person;
import org.datanucleus.api.jdo.JDODataStoreCache;
import org.datanucleus.cache.Level2Cache;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.nscuro.datanucleus.cache.caffeine.CaffeineCachePropertyNames.PROPERTY_CACHE_L2_CAFFEINE_EXPIRY_MODE;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.datanucleus.PropertyNames.PROPERTY_CACHE_L2_EXPIRY_MILLIS;
import static org.datanucleus.PropertyNames.PROPERTY_CACHE_L2_MAXSIZE;
import static org.datanucleus.PropertyNames.PROPERTY_CACHE_L2_STATISTICS_ENABLED;
import static org.datanucleus.PropertyNames.PROPERTY_CACHE_L2_TYPE;
import static org.datanucleus.PropertyNames.PROPERTY_CONNECTION_DRIVER_NAME;
import static org.datanucleus.PropertyNames.PROPERTY_CONNECTION_PASSWORD;
import static org.datanucleus.PropertyNames.PROPERTY_CONNECTION_URL;
import static org.datanucleus.PropertyNames.PROPERTY_CONNECTION_USER_NAME;
import static org.datanucleus.PropertyNames.PROPERTY_PERSISTENCE_UNIT_NAME;
import static org.datanucleus.PropertyNames.PROPERTY_SCHEMA_GENERATE_DATABASE_CREATE_SCRIPT;
import static org.datanucleus.PropertyNames.PROPERTY_SCHEMA_GENERATE_DATABASE_MODE;

class CaffeineCacheTest {

    private static org.testcontainers.containers.PostgreSQLContainer<?> postgresContainer;
    private PersistenceManagerFactory pmf;

    @BeforeAll
    static void beforeAll() {
        postgresContainer = new org.testcontainers.containers.PostgreSQLContainer<>("postgres:16-alpine");
        postgresContainer.start();
    }

    @AfterEach
    void afterEach() {
        if (pmf != null) {
            pmf.close();
        }
    }

    @AfterAll
    static void afterAll() {
        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }

    @Test
    void test() {
        pmf = createPmf(Collections.emptyMap());

        final var jdoDataStoreCache = (JDODataStoreCache) pmf.getDataStoreCache();
        assertThat(jdoDataStoreCache).isNotNull();

        final var secondLevelCache = jdoDataStoreCache.getLevel2Cache();
        assertThat(secondLevelCache).isInstanceOf(CaffeineLevel2Cache.class);
    }

    @Test
    void testMaxSize() {
        pmf = createPmf(Map.of(PROPERTY_CACHE_L2_MAXSIZE, "100"));

        try (final PersistenceManager pm = pmf.getPersistenceManager()) {
            for (int i = 0; i < 150; i++) {
                final var person = new Person();
                person.setName("name-" + i);
                pm.makePersistent(person);
            }
        }

        final Level2Cache secondLevelCache = ((JDODataStoreCache) pmf.getDataStoreCache()).getLevel2Cache();
        assertThat(secondLevelCache.getSize()).isEqualTo(100);
    }

    @Test
    void testExpiryAfterWrite() {
        pmf = createPmf(Map.of(
                PROPERTY_CACHE_L2_EXPIRY_MILLIS, "3000",
                PROPERTY_CACHE_L2_CAFFEINE_EXPIRY_MODE, "after-write",
                PROPERTY_CACHE_L2_STATISTICS_ENABLED, "true"));

        try (final PersistenceManager pm = pmf.getPersistenceManager()) {
            for (int i = 0; i < 10; i++) {
                final var person = new Person();
                person.setName("name-" + i);
                pm.makePersistent(person);
            }
        }

        final var secondLevelCache = (CaffeineLevel2Cache) ((JDODataStoreCache) pmf.getDataStoreCache()).getLevel2Cache();
        assertThat(secondLevelCache.getSize()).isEqualTo(10);

        await("Cache entry expiry")
                .atMost(Duration.ofSeconds(4))
                .untilAsserted(() -> {
                    assertThat(secondLevelCache.getSize()).isEqualTo(0);
                    assertThat(secondLevelCache.getCaffeineCache().stats().evictionCount()).isEqualTo(10);
                });
    }

    @Test
    void testExpiryAfterAccess() {
        pmf = createPmf(Map.of(
                PROPERTY_CACHE_L2_EXPIRY_MILLIS, "3000",
                PROPERTY_CACHE_L2_CAFFEINE_EXPIRY_MODE, "after-access",
                PROPERTY_CACHE_L2_STATISTICS_ENABLED, "true"));

        final var firstObjectId = new AtomicLong(0);
        try (final PersistenceManager pm = pmf.getPersistenceManager()) {
            for (int i = 0; i < 10; i++) {
                final var person = new Person();
                person.setName("name-" + i);
                pm.makePersistent(person);

                if (i == 0) {
                    firstObjectId.set(person.getId());
                }
            }
        }

        final var secondLevelCache = (CaffeineLevel2Cache) ((JDODataStoreCache) pmf.getDataStoreCache()).getLevel2Cache();
        assertThat(secondLevelCache.getSize()).isEqualTo(10);

        await("Cache entry expiry")
                .atMost(Duration.ofSeconds(4))
                .failFast(() -> secondLevelCache.getSize() == 0)
                .pollDelay(Duration.ofMillis(250))
                .untilAsserted(() -> {
                    // Keep accessing the first object so it stays in the cache.
                    try (final PersistenceManager pm = pmf.getPersistenceManager()) {
                        final Person person = pm.getObjectById(Person.class, firstObjectId.get());
                        assertThat(person).isNotNull();
                    }

                    assertThat(secondLevelCache.getSize()).isEqualTo(1);
                    assertThat(secondLevelCache.getCaffeineCache().stats().evictionCount()).isEqualTo(9);
                });
    }

    @Test
    void testEvictAllByClass() {
        pmf = createPmf(Collections.emptyMap());

        try (final PersistenceManager pm = pmf.getPersistenceManager()) {
            for (int i = 0; i < 10; i++) {
                final var person = new Person();
                person.setName("name-" + i);
                pm.makePersistent(person);
            }
        }

        final var secondLevelCache = (CaffeineLevel2Cache) ((JDODataStoreCache) pmf.getDataStoreCache()).getLevel2Cache();
        assertThat(secondLevelCache.getSize()).isEqualTo(10);

        secondLevelCache.evictAll(Person.class, false);
        assertThat(secondLevelCache.getSize()).isEqualTo(0);
    }

    @Test
    void testClose() {
        pmf = createPmf(Collections.emptyMap());

        try (final PersistenceManager pm = pmf.getPersistenceManager()) {
            for (int i = 0; i < 10; i++) {
                final var person = new Person();
                person.setName("name-" + i);
                pm.makePersistent(person);
            }
        }

        final var secondLevelCache = (CaffeineLevel2Cache) ((JDODataStoreCache) pmf.getDataStoreCache()).getLevel2Cache();
        assertThat(secondLevelCache.getSize()).isEqualTo(10);

        secondLevelCache.close();
        assertThat(secondLevelCache.getSize()).isEqualTo(0);
    }

    private PersistenceManagerFactory createPmf(final Map<String, String> configOverrides) {
        final URL schemaUrl = CaffeineCacheTest.class.getResource("/schema.sql");
        assertThat(schemaUrl).isNotNull();

        final Map<String, String> config = new HashMap<>(Map.ofEntries(
                entry(PROPERTY_PERSISTENCE_UNIT_NAME, "test"),
                entry(PROPERTY_SCHEMA_GENERATE_DATABASE_MODE, "drop-and-create"),
                entry(PROPERTY_SCHEMA_GENERATE_DATABASE_CREATE_SCRIPT, schemaUrl.toString()),
                entry(PROPERTY_CONNECTION_URL, postgresContainer.getJdbcUrl()),
                entry(PROPERTY_CONNECTION_DRIVER_NAME, postgresContainer.getDriverClassName()),
                entry(PROPERTY_CONNECTION_USER_NAME, postgresContainer.getUsername()),
                entry(PROPERTY_CONNECTION_PASSWORD, postgresContainer.getPassword()),
                entry(PROPERTY_CACHE_L2_TYPE, "caffeine")));
        config.putAll(configOverrides);

        return JDOHelper.getPersistenceManagerFactory(config, "test");
    }

}
