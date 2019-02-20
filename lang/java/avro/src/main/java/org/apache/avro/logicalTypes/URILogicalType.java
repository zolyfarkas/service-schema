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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;

/**
 * Decimal represents arbitrary-precision fixed-scale decimal numbers
 */
public final class URILogicalType extends AbstractLogicalType<URI> {

  URILogicalType(Schema schema) {
    super(schema.getType(), Collections.EMPTY_SET, "uri",
            Collections.EMPTY_MAP, URI.class);
  }

  @Override
  public URI deserialize(Object object) {
    try {
      CharSequence strVal = (CharSequence) object;
      return new URI(strVal.toString());
    } catch (URISyntaxException ex) {
      throw new AvroRuntimeException("Invalid URL " + object, ex);
    }
  }

  @Override
  public Object serialize(URI url) {
    return url.toString();
  }

}
