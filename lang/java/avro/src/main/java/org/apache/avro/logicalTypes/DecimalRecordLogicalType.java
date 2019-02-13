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
package org.apache.avro.logicalTypes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 *
 * @author Zoltan Farkas
 */
public final class DecimalRecordLogicalType extends DecimalBase {

  private final int scaleIdx;

  private final int unscaledIdx;

  private final Schema schema;

  public DecimalRecordLogicalType(Number precision, Number scale, Schema schema,
          RoundingMode serRm, RoundingMode deserRm) {
    super(precision, scale, schema.getType(), serRm, deserRm);
    Schema.Field field = schema.getField("scale");
    if (field == null || field.schema().getType() != Schema.Type.INT) {
      throw new IllegalArgumentException("Schema " +  schema + " must have int field: scale");
    }
    scaleIdx = field.pos();
    field = schema.getField("unscaled");
    if (field == null || field.schema().getType() != Schema.Type.BYTES) {
      throw new IllegalArgumentException("Schema " +  schema + " must have bytes field: unscaled");
    }
    unscaledIdx = field.pos();
    this.schema = schema;
  }

  @Override
  public BigDecimal doDeserialize(final Object object) {
    GenericRecord rec = (GenericRecord) object;
    Object unscaled = rec.get(unscaledIdx);
    BigInteger unscaledBi;
    if (unscaled instanceof byte[]) {
       unscaledBi = new BigInteger((byte[]) unscaled);
    } else if (unscaled instanceof ByteBuffer) {
      ByteBuffer bb = (ByteBuffer) unscaled;
      byte[] usa = new byte[bb.remaining()];
      bb.get(usa);
      unscaledBi = new BigInteger(usa);
    } else {
      throw new IllegalStateException("Unsupported unscaled value "+  unscaled);
    }
    return new BigDecimal(unscaledBi, (int) rec.get(scaleIdx));
  }

  @Override
  public Object doSerialize(final BigDecimal decimal) {
    GenericRecord rec = new GenericData.Record(schema);
    rec.put(scaleIdx, decimal.scale());
    rec.put(unscaledIdx, ByteBuffer.wrap(decimal.unscaledValue().toByteArray()));
    return rec;
  }


}
