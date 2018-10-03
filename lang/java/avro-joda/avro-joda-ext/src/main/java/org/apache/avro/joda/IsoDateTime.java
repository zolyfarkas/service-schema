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

import java.util.Collections;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.Schema;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class IsoDateTime extends AbstractLogicalType<DateTime> {

  public IsoDateTime(Schema.Type type) {
    super(type, Collections.EMPTY_SET, "isodatetime", Collections.EMPTY_MAP, DateTime.class);
  }

  @Override
  public void validate(Schema schema) {
    // validate the type
    if (schema.getType() != Schema.Type.LONG
            && schema.getType() != Schema.Type.STRING) {
      throw new IllegalArgumentException(
              "Logical type " + this + " must be backed by long or string");
    }
  }


  public static final DateTimeFormatter FMT = ISODateTimeFormat.dateTime().withOffsetParsed();

  public static final DateTimeFormatter PARSER_FMT = ISODateTimeFormat.dateTimeParser().withOffsetParsed();


  @Override
  public DateTime deserialize(Object object) {
    switch (type) {
      case STRING:
        return PARSER_FMT.parseDateTime(object.toString());
      case LONG:
        return new DateTime((Long) object);
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public Object serialize(DateTime object) {
    switch (type) {
      case STRING:
        return FMT.print(object);
      case LONG:
        return object.getMillis();
      default:
        throw new UnsupportedOperationException();
    }
  }

}
