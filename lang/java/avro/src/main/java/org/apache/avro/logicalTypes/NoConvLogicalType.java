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
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.Schema;

/**
 * logical type implementation where no conversion is necessary.
 */
public final class NoConvLogicalType<T> extends AbstractLogicalType<T> {

  NoConvLogicalType(Schema schema, String logicalTypeName, final Class<T> clasz) {
    super(schema.getType(), Collections.EMPTY_SET, logicalTypeName,
            Collections.EMPTY_MAP, clasz);
  }

  @Override
  public T deserialize(Object obj) {
    return (T) obj;
  }

  @Override
  public Object serialize(T obj) {
    return obj;
  }

}
