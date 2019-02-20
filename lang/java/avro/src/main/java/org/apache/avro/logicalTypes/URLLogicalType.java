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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;

/**
 * Decimal represents arbitrary-precision fixed-scale decimal numbers
 */
public final class URLLogicalType extends AbstractLogicalType<URL> {

  URLLogicalType(Schema schema) {
    super(schema.getType(), Collections.EMPTY_SET, "url",
            Collections.EMPTY_MAP, URL.class);
  }

  @Override
  public URL deserialize(Object object) {
    try {
      CharSequence strVal = (CharSequence) object;
      return new URL(strVal.toString());
    } catch (MalformedURLException ex) {
      throw new AvroRuntimeException("Invalid URL " + object, ex);
    }
  }

  @Override
  public Object serialize(URL url) {
    return url.toString();
  }

}
