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
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.Temporal;
import java.util.Collections;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.Schema;

/**
 * Decimal represents arbitrary-precision fixed-scale decimal numbers
 */
public final class AnyTemporal extends AbstractLogicalType<Temporal> {


  AnyTemporal(Schema.Type type) {
    super(type, Collections.EMPTY_SET, "any_temporal",
            Collections.EMPTY_MAP, Temporal.class);
    if (type != Schema.Type.STRING) {
       throw new IllegalArgumentException(this.logicalTypeName + " must be backed by string, not" + type);
    }
  }

  public static int indexOf(final CharSequence cs, final int from, final int to, final char c) {
    for (int i = from; i < to; i++) {
      if (c == cs.charAt(i)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public Temporal deserialize(Object object) {
    switch (type) {
      case STRING:
        CharSequence strVal = (CharSequence) object;
        int l = strVal.length();
        int idx = indexOf(strVal, 0, l, '-');
        if (idx == 0) { // BC Year
          idx = indexOf(strVal, 1, l, '-');
        }
        if (idx < 0) {
          return Year.parse(strVal);
        }
        idx = indexOf(strVal, idx + 1, l, '-');
        if (idx < 0) {
          return YearMonth.parse(strVal);
        }
        idx = indexOf(strVal, idx + 1, l, 'T');
        if (idx <  0) {
          return LocalDate.parse(strVal);
        }
        return LocalDateTime.parse(strVal);
      default:
        throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
    }

  }

  @Override
  public Object serialize(Temporal temporal) {
    switch (type) {
      case STRING:
        return temporal.toString();
      default:
        throw new UnsupportedOperationException("Unsupported type " + type + " for " + this);
    }
  }

}
