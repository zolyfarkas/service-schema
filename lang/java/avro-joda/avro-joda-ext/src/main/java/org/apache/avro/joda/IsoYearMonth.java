/*
 * Copyright 2014 The Apache Software Foundation.
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
package org.apache.avro.joda;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Collections;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.YearMonth;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class IsoYearMonth extends AbstractLogicalType<YearMonth> {

  private final Schema schema;
  private final Class<?> specificType;
  IsoYearMonth(Schema schema) {
    super(schema.getType(), Collections.EMPTY_SET, "isoyearmonth", Collections.EMPTY_MAP, YearMonth.class);
    if (type != Schema.Type.INT && type != Schema.Type.LONG
            && type != Schema.Type.STRING && type != Schema.Type.RECORD) {
      throw new IllegalArgumentException(
              "Logical type " + this + " must be backed by long or int or string");
    }
    this.schema = schema;
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
  }

  public static final DateTimeFormatter FMT = ISODateTimeFormat.yearMonth()
          .withChronology(ISOChronology.getInstanceUTC());

  // This can be anything really, the number serialized will be the number of days between this date and the date.
  // this serialized number will be negative for anything before 1970-01-01.
  private static final LocalDate EPOCH = new LocalDate(0L, DateTimeZone.UTC);

  private static final LoadingCache<YearMonth, String> D2S_CONV_CACHE = CacheBuilder.newBuilder()
          .concurrencyLevel(16)
          .maximumSize(2048)
          .build(new CacheLoader<YearMonth, String>() {

            @Override
            public String load(final YearMonth key) {
              return FMT.print(key);
            }
          });

  private static final LoadingCache<String, YearMonth> S2D_CONV_CACHE = CacheBuilder.newBuilder()
          .concurrencyLevel(16)
          .maximumSize(2048)
          .build(new CacheLoader<String, YearMonth>() {

            @Override
            public YearMonth load(final String key) {
              LocalDate ld = FMT.parseLocalDate(key);
              return new YearMonth(ld.getYear(), ld.getMonthOfYear());
            }
          });

  @Override
  public YearMonth deserialize(Object object) {
    switch (type) {
      case STRING:
        return S2D_CONV_CACHE.getUnchecked(object.toString());
      case LONG: // start of day millis
        DateTime dt = new DateTime((Long) object, DateTimeZone.UTC);
        return  new YearMonth(dt.getYear(), dt.getMonthOfYear());
      case INT: // nr of days since epoch
        LocalDate ld = EPOCH.plusDays((Integer) object);
        return  new YearMonth(ld.getYear(), ld.getMonthOfYear());
      case RECORD:
        GenericRecord rec = (GenericRecord) object;
        return new YearMonth((int) rec.get("year"), (int) rec.get("monthOfYear"));
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public Object serialize(YearMonth object) {
    switch (type) {
      case STRING:
        return D2S_CONV_CACHE.getUnchecked(object);
      case LONG:
        return object.toLocalDate(1).toDateTimeAtStartOfDay(DateTimeZone.UTC).getMillis();
      case INT:
        return Days.daysBetween(EPOCH, object).getDays();
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
        rec.put("year", object.getYear());
        rec.put("monthOfYear", object.getMonthOfYear());
        return rec;
      default:
        throw new UnsupportedOperationException();
    }
  }

}
