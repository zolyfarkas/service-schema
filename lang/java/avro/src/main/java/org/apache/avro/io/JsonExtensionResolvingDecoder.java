/*
 * Copyright 2018 The Apache Software Foundation.
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
package org.apache.avro.io;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;
import org.codehaus.jackson.JsonNode;

/**
 *
 * @author Zoltan Farkas
 */
public class JsonExtensionResolvingDecoder  extends ResolvingDecoder
implements JsonExtensionDecoder {

  private JsonExtensionDecoder extDec;

  public
  JsonExtensionResolvingDecoder(Schema writer, Schema reader, Decoder in) throws IOException {
    super(writer, reader, (Decoder) in);
    extDec = (JsonExtensionDecoder) in;
  }


  @Override
  public <T> T readValue(Schema schema, Class<T> clasz) throws IOException {
    advanceBy(schema);
    return extDec.readValue(schema, clasz);
  }

  @Override
  public BigInteger readBigInteger(Schema schema) throws IOException {
    advanceBy(schema);
    return extDec.readBigInteger(schema);
  }

  @Override
  public BigDecimal readBigDecimal(Schema schema) throws IOException {
    advanceBy(schema);
    return extDec.readBigDecimal(schema);
  }

  public void advanceBy(final Schema schema) throws IOException {
    Schema.Type type = schema.getType();
    switch (type) {
        case BYTES:
          parser.advance(Symbol.BYTES);
          break;
        case STRING:
          parser.advance(Symbol.STRING);
          break;
        case LONG:
          parser.advance(Symbol.LONG);
          break;
        case INT:
          parser.advance(Symbol.INT);
          break;
        default:
          // hack, works for my use cases...
          Symbol rootSymbol = JsonGrammarGenerator.getRootSymbol(schema);
          for (int i = rootSymbol.production.length - 1; i > 0; i--) {
              Symbol s = rootSymbol.production[i];
              if (s.kind == Symbol.Kind.TERMINAL) {
                parser.skipTerminal(s);
              }
          }
      }
  }

  @Override
  public ValidatingDecoder configure(Decoder in) throws IOException {
    extDec = (JsonExtensionDecoder) in;
    return super.configure(in);
  }

  @Override
  public JsonNode readValueAsTree(Schema schema) throws IOException {
    advanceBy(schema);
    return extDec.readValueAsTree(schema);
  }



}
