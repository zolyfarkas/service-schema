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
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.Collections;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.Schema;
import org.threeten.extra.YearQuarter;

/**
 * Any temporal type.
 *
 * can be:
 *
 * Year, YearQuarter, YearMonth, LocalDate, LocalDateTime, ZonedDateTime.
 *
 * Once YearWeek implements Temporal (https://github.com/ThreeTen/threeten-extra/issues/115)
 * it will be added.
 */
public final class AnyTemporalLogicalType extends AbstractLogicalType<Temporal> {


  AnyTemporalLogicalType(Schema.Type type) {
    super(type, Collections.EMPTY_SET, "any_temporal",
            Collections.EMPTY_MAP, Temporal.class);
    if (type != Schema.Type.STRING) {
       throw new IllegalArgumentException(this.logicalTypeName + " must be backed by string, not" + type);
    }
  }

  private static int indexOf(final CharSequence cs, final int from, final int to, final char c) {
    for (int i = from; i < to; i++) {
      if (c == cs.charAt(i)) {
        return i;
      }
    }
    return -1;
  }

  private static int indexOfZone(final CharSequence cs, final int from, final int to) {
    for (int i = from; i < to; i++) {
      char c = cs.charAt(i);
      if (c == ':' || c  == '.' || Character.isDigit(c)) {
        continue;
      } else {
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
        int mIdx = idx + 1;
        idx = indexOf(strVal, mIdx, l, '-');
        if (idx < 0) {
          if (strVal.charAt(mIdx) == 'Q') {
            return YearQuarter.parse(strVal);
          } else {
            return YearMonth.parse(strVal);
          }
        }
        idx = indexOf(strVal, idx + 1, l, 'T');
        if (idx <  0) {
          return LocalDate.parse(strVal);
        }
        idx = indexOfZone(strVal, idx + 1, l);
        if (idx < 0) {
          return LocalDateTime.parse(strVal);
        } else {
          return ZonedDateTime.parse(strVal);
        }
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
