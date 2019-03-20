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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import org.apache.avro.AbstractLogicalType;
import org.apache.avro.AvroNamesRefResolver;
import org.apache.avro.Schema;
import org.apache.avro.SchemaResolver;
import org.apache.avro.data.RawJsonString;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonExtensionDecoder;
import org.apache.avro.io.JsonExtensionEncoder;

/**
 * Decimal represents arbitrary-precision fixed-scale decimal numbers
 */
public final class SchemaLogicalTypeString extends AbstractLogicalType<Schema> {

  private final SchemaResolver resolver;



  SchemaLogicalTypeString(final Schema.Type type, final String logicalTypeName, final SchemaResolver resolver) {
    super(type, Collections.EMPTY_SET, logicalTypeName, Collections.EMPTY_MAP, Schema.class);
    this.resolver = resolver;
  }

  @Override
  public Schema deserialize(Object object) {
      return new Schema.Parser(new AvroNamesRefResolver(resolver)).parse(object.toString());
  }

  @Override
  public Object serialize(Schema schema) {
      return schema.toString();
  }

  @Override
  public Schema tryDirectDecode(Decoder dec, final Schema schema) throws IOException {
    if (dec instanceof JsonExtensionDecoder) {
      JsonExtensionDecoder pd = (JsonExtensionDecoder) dec;
      JsonNode nodes = pd.readValueAsTree(schema);
      return Schema.parse(nodes, new AvroNamesRefResolver(resolver),  true);
    } else {
      return null;
    }
  }

  public String toString(Schema schema) throws UncheckedIOException {
    return AnyAvroLogicalType.toString(schema, resolver);
  }

  @Override
  public boolean tryDirectEncode(Schema object, Encoder enc, final Schema schema) throws IOException {
    if (enc instanceof JsonExtensionEncoder) {
      ((JsonExtensionEncoder) enc).writeValue(new RawJsonString(toString(object)), schema);
      return true;
    } else {
      return false;
    }
  }
}
