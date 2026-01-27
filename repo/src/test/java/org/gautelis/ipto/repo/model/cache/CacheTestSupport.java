/*
 * Copyright (C) 2025-2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.ipto.repo.model.cache;

import com.github.benmanes.caffeine.cache.Cache;
import org.gautelis.ipto.repo.model.Configuration;
import org.gautelis.ipto.repo.model.Context;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

final class CacheTestSupport {
    private CacheTestSupport() {}

    static void resetCacheState() throws Exception {
        Field cacheField = UnitFactory.class.getDeclaredField("unitCache");
        cacheField.setAccessible(true);
        cacheField.set(null, null);

        Field maxSizeField = UnitFactory.class.getDeclaredField("cacheMaxSize");
        maxSizeField.setAccessible(true);
        maxSizeField.setInt(null, -1);

        Field idleMsField = UnitFactory.class.getDeclaredField("cacheIdleCheckIntervalMs");
        idleMsField.setAccessible(true);
        idleMsField.setInt(null, -1);
    }

    static Cache<String, UnitCacheEntry> ensureCache(Context ctx) throws Exception {
        Method method = UnitFactory.class.getDeclaredMethod("ensureCache", Context.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Cache<String, UnitCacheEntry> cache = (Cache<String, UnitCacheEntry>) method.invoke(null, ctx);
        return cache;
    }

    static Configuration newConfig(int maxSize, int idleCheckIntervalSeconds) {
        TestConfig configHandler = new TestConfig(maxSize, idleCheckIntervalSeconds);
        return configHandler.proxy();
    }

    static TestConfig newConfigHandler(int maxSize, int idleCheckIntervalSeconds) {
        return new TestConfig(maxSize, idleCheckIntervalSeconds);
    }

    static final class TestConfig implements InvocationHandler {
        private volatile int maxSize;
        private volatile int idleCheckIntervalSeconds;

        private TestConfig(int maxSize, int idleCheckIntervalSeconds) {
            this.maxSize = maxSize;
            this.idleCheckIntervalSeconds = idleCheckIntervalSeconds;
        }

        Configuration proxy() {
            return (Configuration) Proxy.newProxyInstance(
                    Configuration.class.getClassLoader(),
                    new Class<?>[] { Configuration.class },
                    this
            );
        }

        void setMaxSize(int maxSize) {
            this.maxSize = maxSize;
        }

        void setIdleCheckInterval(int idleCheckIntervalSeconds) {
            this.idleCheckIntervalSeconds = idleCheckIntervalSeconds;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            return switch (method.getName()) {
                case "cacheMaxSize" -> maxSize;
                case "cacheIdleCheckInterval" -> idleCheckIntervalSeconds;
                case "cacheLookBehind" -> true;
                default -> defaultValue(method.getReturnType());
            };
        }

        private Object defaultValue(Class<?> returnType) {
            if (!returnType.isPrimitive()) {
                return null;
            }
            if (returnType == boolean.class) {
                return false;
            }
            if (returnType == byte.class) {
                return (byte) 0;
            }
            if (returnType == short.class) {
                return (short) 0;
            }
            if (returnType == int.class) {
                return 0;
            }
            if (returnType == long.class) {
                return 0L;
            }
            if (returnType == float.class) {
                return 0f;
            }
            if (returnType == double.class) {
                return 0d;
            }
            if (returnType == char.class) {
                return '\0';
            }
            return null;
        }
    }
}
