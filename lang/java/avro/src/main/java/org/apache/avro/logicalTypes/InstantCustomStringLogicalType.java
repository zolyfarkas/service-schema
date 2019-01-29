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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.Schema;

/**
 * Decimal represents arbitrary-precision fixed-scale decimal numbers
 */
public final class InstantCustomStringLogicalType extends AbstractLogicalType<Instant> {

  private final DateTimeFormatter parseFormatter;
  private final DateTimeFormatter outputFormatter;
  private final String format;

  InstantCustomStringLogicalType(Schema schema, String format) {
    super(schema.getType(), Collections.EMPTY_SET, "instant",
            Collections.EMPTY_MAP, Instant.class);
    this.parseFormatter = DateTimeFormatter.ofPattern(format);
    this.outputFormatter = parseFormatter.withZone(ZoneId.of("Z"));
    this.format = format;
  }

  @Override
  public Instant deserialize(Object object) {
    CharSequence strVal = (CharSequence) object;
    return parseFormatter.parse(strVal, Instant::from);
  }

  @Override
  public Object serialize(Instant temporal) {
    StringBuilder builder = new StringBuilder(format.length());
    outputFormatter.formatTo(temporal, builder);
    return builder.toString();
  }

}
