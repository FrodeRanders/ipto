/*
 * Copyright (C) 2024-2025 Frode Randers
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
package org.gautelis.ipto.repo.utils;

import org.gautelis.ipto.repo.exceptions.InvalidParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 *
 */
public class PluginsHelper {
    private static final Logger log = LoggerFactory.getLogger(PluginsHelper.class);

    public static <T> Optional<T> getPlugin(String className, Class<T> clazz) {
        try {
            Class<?> pluginClass = createClass(className);
            return Optional.of(createObject(className, pluginClass));

        } catch (ClassNotFoundException cnfe) {
            log.error("Failed to load plugin: {}", className, cnfe);
            return Optional.empty();
        }
    }

    public static <T> Optional<T> getPlugin(String className, Class<T> clazz, Object... parameters) {
        try {
            Class<?> pluginClass = createClass(className);
            return Optional.of(createObject(className, pluginClass, parameters));

        } catch (ClassNotFoundException cnfe) {
            log.error("Failed to load plugin: {}", className, cnfe);
            return Optional.empty();
        }
    }

    /**
     * Dynamically loads the named class (fully qualified classname).
     */
    public static Class<?> createClass(String className) throws ClassNotFoundException {

        try {
            return Class.forName(className);

        } catch (ExceptionInInitializerError eiie) {
            String info = "Could not load class: " + className
                    + ". Could not initialize static object in server: ";
            info += eiie.getMessage();
            throw new ClassNotFoundException(info, eiie);

        } catch (LinkageError le) {
            String info = "Could not load class: " + className
                    + ". This class is depending on a class that has been changed after compilation ";
            info += "or a class that was not found: ";
            info += le.getMessage();
            throw new ClassNotFoundException(info, le);

        } catch (ClassNotFoundException cnfe) {
            String info = "Could not find class: " + className + ": ";
            info += cnfe.getMessage();
            throw new ClassNotFoundException(info, cnfe);
        }
    }

    /**
     * @param className
     * @param clazz
     * @return Some object
     * @throws ClassNotFoundException
     */
    private static <T> T createObject(
            String className, Class<?> clazz
    ) throws ClassNotFoundException {
        try {
            //noinspection unchecked
            return (T) clazz.getDeclaredConstructor().newInstance();

        } catch (NoSuchMethodException | InstantiationException ie) {
            String buf = "Could not create object: " + className +
                    ". Could not access default constructor";
            throw new ClassNotFoundException(buf, ie);

        } catch (IllegalAccessException iae) {
            String buf = "Could not create object: " + className +
                    ". Could not instantiate object. Does the classname refer to an abstract class, " +
                    "an interface or the like?";
            throw new ClassNotFoundException(buf, iae);

        } catch (ClassCastException cce) {
            String buf = "Could not create object: " + className +
                    ". The specified classname does not refer to the proper type";
            throw new ClassNotFoundException(buf, cce);

        } catch (InvocationTargetException ite) {
            String buf = "Could not create object: " + className +
                    ". Default constructor threw an exception";
            throw new ClassNotFoundException(buf, ite);
        }
    }


    /**
     * @param className
     * @param clazz
     * @param parameters
     * @return Some object
     * @throws ClassNotFoundException
     */
    private static <T> T createObject(
            String className, Class<?> clazz, Object... parameters
    ) throws ClassNotFoundException {
        try {
            Constructor<?> ctor = clazz.getDeclaredConstructor();

            Class<?>[] types = ctor.getParameterTypes();
            if (types.length != parameters.length) {
                throw new InvalidParameterException("Wrong number of arguments to '" + className + "' constructor");
            }

            int i = 0;
            for (Object param : parameters) {
                if (! types[i].isAssignableFrom(param.getClass())) {
                    throw new InvalidParameterException(className + " constructor parameter[" + i + "] has wrong type");
                }
            }

            //noinspection unchecked
            return (T) ctor.newInstance(parameters);

        } catch (NoSuchMethodException | InstantiationException ie) {
            String buf = "Could not create object: " + className +
                    ". Could not access default constructor";
            throw new ClassNotFoundException(buf, ie);

        } catch (IllegalAccessException iae) {
            String buf = "Could not create object: " + className +
                    ". Could not instantiate object. Does the classname refer to an abstract class, " +
                    "an interface or the like?";
            throw new ClassNotFoundException(buf, iae);

        } catch (ClassCastException cce) {
            String buf = "Could not create object: " + className +
                    ". The specified classname does not refer to the proper type";
            throw new ClassNotFoundException(buf, cce);

        } catch (InvocationTargetException ite) {
            String buf = "Could not create object: " + className +
                    ". Default constructor threw an exception";
            throw new ClassNotFoundException(buf, ite);
        }
    }
}
