/*
 * Copyright 2020 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.util;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.function.Function;

public enum ReferenceType {
  WEAK((Object object) -> new WeakReference<>(object)),
  SOFT((Object object) -> new SoftReference<>(object));

  private final Function<Object, Reference<Object>> factory;

  ReferenceType(final Function<Object, Reference<Object>> factory) {
    this.factory = factory;
  }

  public <T> Reference<T> create(final T object) {
    return (Reference<T>) factory.apply(object);
  }

}
