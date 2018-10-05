/*
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

package org.apache.avro;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.logicalTypes.BigIntegerFactory;
import org.apache.avro.logicalTypes.DecimalFactory;


public class LogicalTypes {

  private static final Map<String, org.apache.avro.LogicalTypeFactory> REGISTERED_TYPES =
      new ConcurrentHashMap<>();


  static {
     register(new DecimalFactory());
     register(new BigIntegerFactory());
     ServiceLoader<org.apache.avro.LogicalTypeFactory> factories
             = ServiceLoader.load(org.apache.avro.LogicalTypeFactory.class);
     Iterator<org.apache.avro.LogicalTypeFactory> iterator = factories.iterator();
     while (iterator.hasNext()) {
        register(iterator.next());
     }
  }

  /**
   * factory for avro official compatibility.
   */
  public interface LogicalTypeFactory  {
    LogicalType fromSchema(Schema schema);
  }

  public static void register(String logicalTypeName, LogicalTypeFactory factory) {
    register(new org.apache.avro.LogicalTypeFactory() {
      @Override
      public String getLogicalTypeName() {
        return logicalTypeName;
      }

      @Override
      public LogicalType create(Schema.Type schemaType, Map<String, Object> attributes) {
        throw new UnsupportedOperationException();
      }

      @Override
      public LogicalType fromSchema(Schema schema) {
        return factory.fromSchema(schema);
      }

    });
  }

  public static void register(@Nonnull org.apache.avro.LogicalTypeFactory factory) {
    org.apache.avro.LogicalTypeFactory ex = REGISTERED_TYPES.putIfAbsent(factory.getLogicalTypeName(), factory);
    if (ex != null) {
      throw new IllegalArgumentException("Already registered " + ex + ", cannot register " + factory);
    }
  }

  /**
   * for avro official compatibility.
   */
  @Nullable
  public static LogicalType fromSchema(Schema schema) {
    String typeName = (String) schema.getProp(LogicalType.LOGICAL_TYPE_PROP);
    if (typeName != null) {
      org.apache.avro.LogicalTypeFactory ltf = REGISTERED_TYPES.get(typeName);
      if (ltf != null) {
        return ltf.fromSchema(schema);
      } else {
        if (Boolean.getBoolean("allowUndefinedLogicalTypes"))  {
          return null;
        } else {
          throw new IllegalArgumentException("Undefined logical type " + schema);
        }
      }
    } else {
      return null;
    }
  }



}
