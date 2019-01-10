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

/**
 * Decimal represents arbitrary-precision fixed-scale decimal numbers
 */
public final class InstantNanoRecordLogicalType extends AbstractLogicalType<Instant> {

  private final Schema schema;

  private final int epochSecondIdx;

  private final int nanoIdx;

  InstantNanoRecordLogicalType(Schema schema) {
    super(schema.getType(), Collections.EMPTY_SET, "instant",
            Collections.EMPTY_MAP, Instant.class);
    Schema.Field esField = schema.getField("epochSecond");
    if (esField == null) {
      throw new IllegalArgumentException("Missing field epochSecond in " + schema);
    }
    epochSecondIdx = esField.pos();
    Schema.Field nField = schema.getField("nano");
    if (esField == null) {
      throw new IllegalArgumentException("Missing field nano in " + schema);
    }
    nanoIdx = nField.pos();
    this.schema = schema;
  }

  @Override
  public Instant deserialize(Object object) {
    GenericRecord rec = (GenericRecord) object;
    return Instant.ofEpochSecond((long) rec.get(epochSecondIdx), (int) rec.get(nanoIdx));
  }

  @Override
  public Object serialize(Instant temporal) {
    GenericRecord rec = new GenericData.Record(schema);
    rec.put(epochSecondIdx, temporal.getEpochSecond());
    rec.put(nanoIdx, temporal.getNano());
    return rec;
  }

}
