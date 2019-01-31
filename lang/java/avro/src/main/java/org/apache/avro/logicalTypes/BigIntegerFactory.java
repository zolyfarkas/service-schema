/*
 * Copyright 2016 The Apache Software Foundation.
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
package org.apache.avro.logicalTypes;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypeFactory;
import org.apache.avro.Schema;

/**
 *
 * @author zfarkas
 */
public class BigIntegerFactory implements LogicalTypeFactory {

  @Override
  public String getLogicalTypeName() {
    return "bigint";
  }

  static Integer getPrecision(final Map<String, Object> attributes) {
    Object prec = attributes.get("precision");
    if (prec == null) {
      return null;
    } else {
      return ((Number) prec).intValue();
    }
  }

  static Map<String, Object> toAttributes(final Integer precision) {
    if (precision == null) {
      return Collections.EMPTY_MAP;
    } else {
      Map<String, Object> res = new HashMap<>(2);
      res.put("precision", precision);
      return res;
    }
  }


  @Override
  public LogicalType create(Schema.Type schemaType, Map<String, Object> attributes) {
    switch (schemaType) {
      case STRING:
        return new BigIntegerString(schemaType, getPrecision(attributes));
      case BYTES:
        return new BigIntegerBytes(schemaType, getPrecision(attributes));
      default:
        throw new IllegalArgumentException("Invalid type for bigint: " + schemaType);
    }
  }

}
