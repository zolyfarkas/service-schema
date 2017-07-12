/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ClassUtils {

  private ClassUtils() {
  }

  /**
   * Loads a class using the class loader.
   * 1. The class loader of the current class is being used.
   * 2. The thread context class loader is being used.
   * If both approaches fail, returns null.
   *
   * @param className The name of the class to load.
   * @return The class or null if no class loader could load the class.
   */
  public static Class<?> forName(String className)
    throws ClassNotFoundException {
    return ClassUtils.forName(ClassUtils.class, className);
  }

  /**
   * Loads a class using the class loader.
   * 1. The class loader of the context class is being used.
   * 2. The thread context class loader is being used.
   * If both approaches fail, returns null.
   *
   * @param contextClass The name of a context class to use.
   * @param className    The name of the class to load
   * @return The class or null if no class loader could load the class.
   */
  public static Class<?> forName(@Nonnull Class<?> contextClass, String className)
    throws ClassNotFoundException {
    return forName(contextClass.getClassLoader(), className);
  }

  /**
   * Loads a class using the class loader.
   * 1. The class loader of the context class is being used.
   * 2. The thread context class loader is being used.
   * If both approaches fail, returns null.
   *
   * @param classLoader The classloader to use.
   * @param className    The name of the class to load
   * @return The class or null if no class loader could load the class.
   */
  public static Class<?> forName(@Nullable ClassLoader classLoader, String className)
    throws ClassNotFoundException {
    Class<?> c = null;
    if (classLoader != null) {
      c = forName(className, classLoader);
      if (c != null) {
        return c;
      }
    }
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    if (contextClassLoader != null) {
      return Class.forName(className, true, contextClassLoader);
    } else {
      throw new ClassNotFoundException("Failed to load class" + className);
    }
  }

  /**
   * Loads a {@link Class} from the specified {@link ClassLoader} without
   * throwing {@link ClassNotFoundException}.
   *
   * @param className
   * @param classLoader
   * @return
   */
  @Nullable
  private static Class<?> forName(@Nonnull String className, @Nonnull ClassLoader classLoader) {
      try {
        return Class.forName(className, true, classLoader);
      } catch (ClassNotFoundException e) {
        return null;
      }
  }
}
