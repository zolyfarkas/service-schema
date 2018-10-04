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
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;


public class LogicalTypes {

  private static final ObjectMapper OM = new ObjectMapper();

  private static final Map<String, LogicalTypeFactory> REGISTERED_TYPES =
      new ConcurrentHashMap<>();


  static {
     register(new DecimalFactory());
     register(new BigIntegerFactory());
     ServiceLoader<LogicalTypeFactory> factories = ServiceLoader.load(LogicalTypeFactory.class);
     Iterator<LogicalTypeFactory> iterator = factories.iterator();
     while (iterator.hasNext()) {
        register(iterator.next());
     }
  }

  public static void register(@Nonnull LogicalTypeFactory factory) {
    LogicalTypeFactory ex = REGISTERED_TYPES.putIfAbsent(factory.getLogicalTypeName(), factory);
    if (ex != null) {
      throw new IllegalArgumentException("Already registered " + ex + ", cannot register " + factory);
    }
  }

  /**
   * @deprecated use create.
   */
  @Deprecated
  public static LogicalType fromSchema(Schema schema) {
    return create(schema.getType(), schema.getObjectProps());
  }

  @Nullable
  public static LogicalType fromJsonNode(JsonNode node, Schema.Type schemaType) {
    final JsonNode logicalTypeNode = node.get("logicalType");
    if (logicalTypeNode == null) {
        return null;
    }
    LogicalType lt = create(schemaType, OM.convertValue(node, Map.class));
    if (lt != null) {
        return lt;
    } else {
      if (Boolean.getBoolean("allowUndefinedLogicalTypes"))  {
        return null;
      } else {
        throw new IllegalArgumentException("Undefined logical type " + logicalTypeNode.asText());
      }
    }
  }

  @Nullable
  public static LogicalType create(Schema.Type schemaType, Map<String, Object> attributes) {
    String typeName = (String) attributes.get(LogicalType.LOGICAL_TYPE_PROP);
    LogicalTypeFactory ltf = REGISTERED_TYPES.get(typeName);
    if (ltf != null) {
      return ltf.create(schemaType, attributes);
    } else {
      return null;
    }
  }


}
