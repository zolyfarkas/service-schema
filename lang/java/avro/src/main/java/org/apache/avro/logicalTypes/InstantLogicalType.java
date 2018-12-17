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

import java.time.Instant;
import java.util.Collections;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

/**
 * Decimal represents arbitrary-precision fixed-scale decimal numbers
 */
public final class InstantLogicalType extends AbstractLogicalType<Instant> {

  private final Class<?> specificType;

  private final Schema schema;

  InstantLogicalType(Schema schema) {
    super(schema.getType(), Collections.EMPTY_SET, "instant",
            Collections.EMPTY_MAP, Instant.class);
    if (type != Schema.Type.STRING && type != Schema.Type.RECORD) {
       throw new IllegalArgumentException(this.logicalTypeName + " must be backed by string, not" + type);
    }
    if (schema.getType() == Schema.Type.RECORD) {
      Class<?> clasz;
      try {
        clasz = Class.forName(schema.getFullName());
      } catch (ClassNotFoundException ex) {
        clasz = null;
      }
      if (clasz != null && SpecificRecord.class.isAssignableFrom(clasz)) {
        specificType = clasz;
      } else {
        specificType = null;
      }
    } else {
      specificType = null;
    }
    this.schema = schema;
  }

  @Override
  public Instant deserialize(Object object) {
    switch (type) {
      case STRING:
        CharSequence strVal = (CharSequence) object;
        return Instant.parse(strVal);
      case RECORD:
        GenericRecord rec = (GenericRecord) object;
        return Instant.ofEpochSecond((long) rec.get("epochSecond"), (int) rec.get("nano"));
      default:
        throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
    }
  }

  @Override
  public Object serialize(Instant temporal) {
    switch (type) {
      case STRING:
        return temporal.toString();
      case RECORD:
        GenericRecord rec;
        if (specificType != null) {
          try {
            rec = (GenericRecord) specificType.newInstance();
          } catch (InstantiationException | IllegalAccessException ex) {
            throw new RuntimeException(ex);
          }
        } else {
          rec = new GenericData.Record(schema);
        }
        rec.put("epochSecond", temporal.getEpochSecond());
        rec.put("nano", temporal.getNano());
        return rec;
      default:
        throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
    }
  }

}
