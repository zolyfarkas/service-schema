package org.apache.avro.io;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

//import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.Symbol;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.util.TokenBuffer;
//import com.fasterxml.jackson.databind.exc.MismatchedInputException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.apache.avro.LogicalType;
import static org.apache.avro.io.JsonDecoder.CHARSET;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.logicalTypes.DecimalBytesLogicalType;

/**
 * This class extends the JsonDecoder to:
 * <ul>
 * <li>Decode JSON with "natural" encoding of union {null, type} encoding</li>
 * <li>Infer not present attributes by using the default defined in the writer schema</li>
 * <li>Ability to decode the decimal logicalType when naturally encoded in JSON.</li>
 * </ul>
 */
public final class ExtendedJsonDecoder extends JsonDecoder
        implements JsonExtensionDecoder {


    private final boolean lenient;

    public ExtendedJsonDecoder(final Schema schema, final InputStream in)
            throws IOException {
        this(schema, in, true);
    }

    public ExtendedJsonDecoder(final Schema schema, final InputStream in, boolean lenient)
            throws IOException {
        super(schema, in);
        this.lenient = lenient;
    }

    public ExtendedJsonDecoder(final Schema schema, final JsonParser in, boolean lenient)
            throws IOException {
        super(schema, in);
        this.lenient = lenient;
    }

    public ExtendedJsonDecoder(final Schema schema, final String in)
            throws IOException {
        this(schema, new ByteArrayInputStream(in.getBytes(Charset.forName("UTF-8"))));
    }

    /**
     * Overwrite this function to support decoding of: union {null, anyType}
     *
     * @return
     * @throws IOException
     */
  @Override
  public int readIndex() throws IOException {
    super.advance(Symbol.UNION);
    JsonParser lin = this.in;
    Symbol.Alternative a = (Symbol.Alternative) parser.popSymbol();

    String label;
    final JsonToken currentToken = lin.getCurrentToken();
    if (currentToken == JsonToken.VALUE_NULL) {
      label = "null";
    } else if (ExtendedJsonEncoder.isNullableSingle(a)) {
      label = ExtendedJsonEncoder.getNullableSingle(a);
    } else if (currentToken == JsonToken.START_OBJECT
            && lin.nextToken() == JsonToken.FIELD_NAME) {
      label = lin.getText();
      lin.nextToken();
      parser.pushSymbol(Symbol.UNION_END);
    } else {
      throw (AvroTypeException) error("start-union");
    }
    int n = a.findLabel(label);
    if (n < 0) {
      throw new AvroTypeException("Unknown union branch " + label);
    }
    parser.pushSymbol(a.getSymbol(n));
    return n;
  }

    /**
     * Overwrite to inject default values.
     *
     * @param input
     * @param top
     * @return
     * @throws IOException
     */

    @Override
    public Symbol doAction(final Symbol input, final Symbol top) throws IOException {
        try {
            JsonParser in = this.in;
            if (top instanceof Symbol.FieldAdjustAction) {
                Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
                String name = fa.fname;
                if (currentReorderBuffer != null) {
                    TokenBuffer node = currentReorderBuffer.savedFields.remove(name);
                    if (node != null) {
                        currentReorderBuffer.origParser = in;
                        this.in = node.asParserOnFirstToken();
                        return null;
                    }
                }
                if (in.getCurrentToken() == JsonToken.FIELD_NAME) {
                    do {
                        String fn = in.getText();
                        in.nextToken();
                        if (name.equals(fn)) {
                            return null;
                        } else {
                            if (currentReorderBuffer == null) {
                                currentReorderBuffer = new JsonDecoder.ReorderBuffer();
                            }
                            TokenBuffer tokenBuffer = TokenBuffer.asCopyOfValue(in);
                            // Moves the parser to the end of the current event e.g. END_OBJECT
                            currentReorderBuffer.savedFields.put(fn, tokenBuffer);
                            in.nextToken();
                        }
                    } while (in.getCurrentToken() == JsonToken.FIELD_NAME);
                    if (injectDefaultValueIfAvailable(in, fa)) {
                        return null;
                    }
                    throw new AvroTypeException("Expected field name not found: " + fa.fname
                            + " instead found " + in.getCurrentToken());
                } else {
                    if (injectDefaultValueIfAvailable(in, fa)) {
                        return null;
                    }
                    throw new AvroTypeException("Expected field name not found: " + fa.fname
                          + " instead found " + in.getCurrentToken());
                }
            } else if (top == Symbol.FIELD_END) {
                if (currentReorderBuffer != null && currentReorderBuffer.origParser != null) {
                    this.in = currentReorderBuffer.origParser;
                    currentReorderBuffer.origParser = null;
                }
            } else if (top == Symbol.RECORD_START) {
                if (in.getCurrentToken() == JsonToken.START_OBJECT) {
                    in.nextToken();
                    reorderBuffers.push(currentReorderBuffer);
                    currentReorderBuffer = null;
                } else {
                    throw error("record-start");
                }
            } else if (top == Symbol.RECORD_END || top == Symbol.UNION_END) {
                if (in.getCurrentToken() == JsonToken.END_OBJECT) {
                    in.nextToken();
                    if (top == Symbol.RECORD_END) {
                        if (!lenient && currentReorderBuffer != null && !currentReorderBuffer.savedFields.isEmpty()) {
                            throw error("Unknown fields: " + currentReorderBuffer.savedFields.keySet());
                        }
                        currentReorderBuffer = reorderBuffers.pop();
                    }
                } else {
                  if (lenient && top == Symbol.RECORD_END) {
                    if (in.getCurrentToken() != JsonToken.END_OBJECT) {
                      int level = 1;
                      while (level > 0) {
                        JsonToken nextToken = in.nextToken();
                        if (nextToken == null) {
                          break;
                        } else if (nextToken == JsonToken.START_OBJECT) {
                          level++;
                        } else if (nextToken == JsonToken.END_OBJECT) {
                          level--;
                        }
                      }
                    }
                    in.nextToken();
                    currentReorderBuffer = reorderBuffers.pop();
                  } else {
                    throw error(top == Symbol.RECORD_END ? "record-end" : "union-end");
                  }
                }
            } else {
                throw new AvroTypeException("Unknown action symbol " + top);
            }
            return null;
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    AvroTypeException error(String type) {
      try {
        return new AvroTypeException("Expected " + type +
                ". Got " + in.getCurrentToken() + " token value = " + in.getText() + " at "+ in.getCurrentLocation());
      } catch (IOException ex) {
        UncheckedIOException ioEx = new UncheckedIOException(ex);
        ioEx.addSuppressed(super.error(type));
        throw ioEx;
      }
    }

    private boolean injectDefaultValueIfAvailable(final JsonParser in, Symbol.FieldAdjustAction action)
            throws IOException, IllegalAccessException {
        JsonNode defVal = action.defaultValue;
        if (null != defVal) {
            JsonParser traverse = defVal.traverse();
            traverse.nextToken();
            TokenBuffer result = TokenBuffer.asCopyOfValue(traverse);
            result.setCodec(Schema.MAPPER);
            if (currentReorderBuffer == null) {
                currentReorderBuffer = new ReorderBuffer();
            }
            currentReorderBuffer.origParser = in;
            this.in = result.asParserOnFirstToken();
            return true;
        }
        return false;
    }

  /**
   * Overwrite to support decimal json decoding
   */
  @Override
  public String readString() throws IOException {
    advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      if (in.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw error("map-key");
      }
    } else {
      JsonToken currentToken = in.getCurrentToken();
      if (currentToken != JsonToken.VALUE_STRING && currentToken != JsonToken.VALUE_NUMBER_INT
              && currentToken != JsonToken.VALUE_NUMBER_FLOAT) {
        throw error("string");
      }
    }
    String result = in.getText();
    in.nextToken();
    return result;
  }

  /**
   * Overwrite to support decimal json decoding
   */
  @Override
  public void skipString() throws IOException {
    advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      if (in.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw error("map-key");
      }
    } else {
      JsonToken currentToken = in.getCurrentToken();
      if (currentToken != JsonToken.VALUE_STRING && currentToken != JsonToken.VALUE_NUMBER_INT
              && currentToken != JsonToken.VALUE_NUMBER_FLOAT) {
        throw error("string");
      }
    }
    in.nextToken();
  }

  /**
   * Overwrite to support decimal json decoding
   */
  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    advance(Symbol.BYTES);
    JsonToken currentToken = in.getCurrentToken();
    switch (currentToken) {
      case VALUE_STRING:
        byte[] result = readByteArray();
        in.nextToken();
        return ByteBuffer.wrap(result);
      case VALUE_NUMBER_INT:
        BigInteger bigIntegerValue = in.getBigIntegerValue();
        in.nextToken();
        return ByteBuffer.wrap(bigIntegerValue.toByteArray());
      case VALUE_NUMBER_FLOAT:
        BigDecimal decimalValue = in.getDecimalValue();
        in.nextToken();
        return DecimalBytesLogicalType.toBytes(decimalValue);
      default:
        throw error("bytes");
    }
  }

  @Override
  public void skipBytes() throws IOException {
    advance(Symbol.BYTES);
    JsonToken currentToken = in.getCurrentToken();
    if (currentToken == JsonToken.VALUE_STRING
            || currentToken == JsonToken.VALUE_NUMBER_INT || currentToken == JsonToken.VALUE_NUMBER_FLOAT) {
      in.nextToken();
    } else {
      throw error("bytes");
    }
  }


  @Override
  public BigInteger readBigInteger(final Schema schema) throws IOException {
      advanceBy(schema);
      JsonToken currentToken = in.getCurrentToken();
      BigInteger result;
      switch (currentToken) {
        case VALUE_STRING:
          result = new BigInteger(in.getText());
          break;
        case VALUE_NUMBER_INT:
          result = in.getBigIntegerValue();
          break;
        default:
          throw new AvroTypeException("Invalid token type " + currentToken + ", expecting a int");
      }
      in.nextToken();
      return result;
  }

  @Override
  public BigDecimal readBigDecimal(final Schema schema) throws IOException {
      advanceBy(schema);
      JsonToken currentToken = in.getCurrentToken();
      BigDecimal result;
      switch (currentToken) {
        case VALUE_STRING:
          result = new BigDecimal(in.getText());
          break;
        case VALUE_NUMBER_INT:
        case VALUE_NUMBER_FLOAT:
          result = in.getDecimalValue();
          break;
        default:
          throw new AvroTypeException("Invalid token type " + currentToken + ", expecting " + schema);
      }
      in.nextToken();
      return result;
  }

  @Override
  public <T> T readValue(final Schema schema, final Class<T> clasz) throws IOException {
    advanceBy(schema);
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      // probably encoded with recular encoder, will be best effort here.
      // this is a horendous way
      String text = in.getText();
      T result;
      LogicalType lt = schema.getLogicalType();
      if (lt != null && "json_any".equals(lt.getLogicalTypeName())) {
          result = (T) text;
      } else {
        switch (schema.getType()) {
          case STRING:
             result = Schema.FACTORY.createParser(text).readValueAs(clasz);
            break;
          case BYTES:
            result = Schema.FACTORY.createParser(text.getBytes(CHARSET)).readValueAs(clasz);
            break;
          default:
            throw new UnsupportedOperationException("Unsupported schema " + schema);
        }
      }
      in.nextToken();
      return result;
    }
    T result = in.readValueAs(clasz);
    in.nextToken();
    return result;
  }

  public void advanceBy(final Schema schema) throws IOException {
    Schema.Type type = schema.getType();
    switch (type) {
        case BYTES:
          advance(Symbol.BYTES);
          break;
        case STRING:
          advance(Symbol.STRING);
          break;
        case LONG:
          advance(Symbol.LONG);
          break;
        case INT:
          advance(Symbol.INT);
          break;
        default:
          Symbol rootSymbol = JsonGrammarGenerator.getRootSymbol(schema);
//          skipSymbols(rootSymbol);
          Parser p = new Parser(rootSymbol, null);
          int countToFirstTerminal = p.countToFirstTerminal();
          Symbol theTerminal = p.lastSymbol();
          int sleft = p.countToEnd();
          int countToFirstTerminal2 = parser.countToFirstTerminal();
          Symbol theTerminal2 = parser.lastSymbol();
          if (theTerminal != theTerminal2) {
            throw new IllegalStateException("expected " +  theTerminal + " got " + theTerminal2);
          }
          parser.goBack(countToFirstTerminal2);
          int advance = countToFirstTerminal2 - countToFirstTerminal;
          parser.advance(advance);
          if (advance >= 0) {
            parser.skip(sleft + countToFirstTerminal - 1);
          } else {
            parser.skip(sleft + countToFirstTerminal2 - 1);
          }
          break;
      }
  }

  private void skipSymbols(Symbol rootSymbol) throws IOException {
    for (int i = rootSymbol.production.length - 1; i > 0; i--) {
      Symbol s = rootSymbol.production[i];
      if (s.kind == Symbol.Kind.TERMINAL) {
        parser.skipTerminal(s);
      } else if (s.production != null && s.production.length > 0) {
        skipSymbols(s);
      }
    }
  }


  @Override
  public JsonNode readValueAsTree(final Schema schema) throws IOException {
    advanceBy(schema);
    JsonNode result = in.readValueAsTree();
    in.nextToken();
    return result;
  }

  @Override
  public TokenBuffer bufferValue(Schema schema) throws IOException {
    advanceBy(schema);
    TokenBuffer result = TokenBuffer.asCopyOfValue(in);
    in.nextToken();
    return result;
  }

  @Override
  public String toString() {
    return "ExtendedJsonDecoder{" + "lenient=" + lenient + " at " + in.getCurrentLocation() + '}';
  }




}
