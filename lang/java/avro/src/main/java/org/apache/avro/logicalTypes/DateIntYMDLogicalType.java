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
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.Schema;

/**
 * Data represented as number like YYYYmmdd.
 */
public final class DateIntYMDLogicalType extends AbstractLogicalType<LocalDate> {

  private static final Map<String, Object> ATTR = new HashMap<>(2);

  static {
    ATTR.put("ymd", Boolean.TRUE);
  }

  DateIntYMDLogicalType(Schema schema) {
    super(schema.getType(), Collections.EMPTY_SET, "date",
            ATTR, LocalDate.class);
  }

  @Override
  public LocalDate deserialize(Object object) {
    int val = ((Number) object).intValue();
    int day = val % 100;
    int month = val % 10000 / 100;
    int year = val / 10000;
    return LocalDate.of(year, month, day);
  }

  @Override
  public Object serialize(LocalDate temporal) {
    return temporal.getYear() * 10000 + temporal.getMonthValue() * 100 + temporal.getDayOfMonth();
  }

}
