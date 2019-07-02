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

import java.time.LocalDate;
import java.util.Collections;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Date representation as  record with fields: year, month day.
 */
public final class DateRecordLogicalType extends AbstractLogicalType<LocalDate> {

  private final Schema schema;

  private final int yearIdx;

  private final int monthIdx;

  private final int dayIdx;

  DateRecordLogicalType(Schema schema) {
    super(schema.getType(), Collections.EMPTY_SET, "date",
            Collections.EMPTY_MAP, LocalDate.class);
    Schema.Field yearField = schema.getField("year");
    if (yearField == null) {
      throw new IllegalArgumentException("Missing field year in " + schema);
    }
    yearIdx = yearField.pos();
    Schema.Field monthField = schema.getField("month");
    if (yearField == null) {
      throw new IllegalArgumentException("Missing field year in " + schema);
    }
    monthIdx = monthField.pos();
    Schema.Field dayField = schema.getField("day");
    if (dayField == null) {
      throw new IllegalArgumentException("Missing field year in " + schema);
    }
    dayIdx = dayField.pos();
    this.schema = schema;
  }

  @Override
  public LocalDate deserialize(Object object) {
    GenericRecord rec = (GenericRecord) object;
    return LocalDate.of((int) rec.get(yearIdx), (int) rec.get(monthIdx), (int) rec.get(dayIdx));
  }

  @Override
  public Object serialize(LocalDate temporal) {
    GenericRecord rec = new GenericData.Record(schema);
    rec.put(yearIdx, temporal.getYear());
    rec.put(monthIdx, temporal.getMonthValue());
    rec.put(dayIdx, temporal.getDayOfMonth());
    return rec;
  }

}
