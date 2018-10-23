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

import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypeFactory;
import org.apache.avro.Schema;

/**
 * @author zfarkas
 */
public class JsonArrayLogicalTypeFactory implements LogicalTypeFactory {

  private final String logicalTypeName = "json_array";

  private final JsonLogicalType lt = new JsonLogicalType(Schema.Type.BYTES, logicalTypeName, List.class);

  @Override
  public String getLogicalTypeName() {
    return logicalTypeName;
  }

  @Override
  public LogicalType create(Schema.Type type, Map<String, Object> attributes) {
    if (type != Schema.Type.BYTES) {
       throw new IllegalArgumentException(this.getLogicalTypeName() + " must be backed by string or bytes, not" + type);
    }
    return lt;
  }

}
