/*
 * Copyright 2019 The Apache Software Foundation.
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
package org.apache.avro;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author Zoltan Farkas
 */
public final class SchemaResolvers {

  private static final  SchemaResolver NO_RESOLVER = new SchemaResolver() {
    @Override
    public Schema resolveSchema(String id) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getId(Schema schema) {
      return null;
    }
  };


  private static final Map<String, SchemaResolver> REGISTERED_RESOLVERS=
      new ConcurrentHashMap<>();

  public static  SchemaResolver get(@Nullable final String name) {
    if (name == null) {
      return getDefault();
    }
    return REGISTERED_RESOLVERS.get(name);
  }

  public static  SchemaResolver register(@Nonnull final String name, final SchemaResolver resolver) {
    return REGISTERED_RESOLVERS.put(name, resolver);
  }

  public static  SchemaResolver registerDefault(final SchemaResolver resolver) {
    return REGISTERED_RESOLVERS.put("def", resolver);
  }

  @Nonnull
  public static SchemaResolver getDefault() {
    SchemaResolver res = REGISTERED_RESOLVERS.get("def");
    if (res == null) {
      return NO_RESOLVER;
    }
    return res;
  }

  private SchemaResolvers() { }

}
