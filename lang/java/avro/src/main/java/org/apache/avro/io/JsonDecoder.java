/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.Base64Variant;
import org.codehaus.jackson.JsonLocation;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonStreamContext;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.ObjectCodec;

/** A {@link Decoder} for Avro's JSON data encoding.
 * </p>
 * Construct using {@link DecoderFactory}.
 * </p>
 * JsonDecoder is not thread-safe.
 * */
public class JsonDecoder extends ParsingDecoder
  implements Parser.ActionHandler {
  protected JsonParser in;
  SimpleStack<ReorderBuffer> reorderBuffers = new SimpleStack<ReorderBuffer>(4);
  ReorderBuffer currentReorderBuffer;

  static class ReorderBuffer {
    public Map<String, List<JsonElement>> savedFields = new HashMap<String, List<JsonElement>>();
    public JsonParser origParser = null;
  }

  static final Charset CHARSET = StandardCharsets.ISO_8859_1;

  private JsonDecoder(Symbol root, InputStream in) throws IOException {
    super(root);
    configure(in);
  }

  private JsonDecoder(Symbol root, String in) throws IOException {
    super(root);
    configure(in);
  }

  JsonDecoder(Schema schema, InputStream in) throws IOException {
    this(JsonGrammarGenerator.getRootSymbol(schema), in);
  }

  JsonDecoder(Schema schema, JsonParser in) throws IOException {
    super(JsonGrammarGenerator.getRootSymbol(schema));
    parser.reset();
    this.in = in;
    this.in.nextToken();
  }

  JsonDecoder(Schema schema, String in) throws IOException {
    this(JsonGrammarGenerator.getRootSymbol(schema), in);
  }


  /**
   * Reconfigures this JsonDecoder to use the InputStream provided.
   * <p/>
   * If the InputStream provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonDecoder will reset its state and then
   * reconfigure its input.
   * @param in
   *   The IntputStream to read from. Cannot be null.
   * @throws IOException
   * @return this JsonDecoder
   */
  private JsonDecoder configure(@Nonnull InputStream in) throws IOException {
    parser.reset();
    this.in = Schema.FACTORY.createJsonParser(in);
    this.in.nextToken();
    return this;
  }

  /**
   * Reconfigures this JsonDecoder to use the String provided for input.
   * <p/>
   * If the String provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonDecoder will reset its state and then
   * reconfigure its input.
   * @param in
   *   The String to read from. Cannot be null.
   * @throws IOException
   * @return this JsonDecoder
   */
  public JsonDecoder configure(@Nonnull String in) throws IOException {
    parser.reset();
    this.in = Schema.FACTORY.createJsonParser(in);
    this.in.nextToken();
    return this;
  }

  protected void advance(Symbol symbol) throws IOException {
    this.parser.processTrailingImplicitActions();
    if (in.getCurrentToken() == null && this.parser.depth() == 1)
      throw new EOFException();
    parser.advance(symbol);
  }

  @Override
  public void readNull() throws IOException {
    advance(Symbol.NULL);
    if (in.getCurrentToken() == JsonToken.VALUE_NULL) {
      in.nextToken();
    } else {
      throw error("null");
    }
  }

  @Override
  public boolean readBoolean() throws IOException {
    advance(Symbol.BOOLEAN);
    JsonToken t = in.getCurrentToken();
    if (t == JsonToken.VALUE_TRUE || t == JsonToken.VALUE_FALSE) {
      in.nextToken();
      return t == JsonToken.VALUE_TRUE;
    } else {
      throw error("boolean");
    }
  }

  @Override
  public int readInt() throws IOException {
    advance(Symbol.INT);
    if (in.getCurrentToken().isNumeric()) {
      int result = in.getIntValue();
      in.nextToken();
      return result;
    } else {
      throw error("int");
    }
  }

  @Override
  public long readLong() throws IOException {
    advance(Symbol.LONG);
    if (in.getCurrentToken().isNumeric()) {
      long result = in.getLongValue();
      in.nextToken();
      return result;
    } else {
      throw error("long");
    }
  }

  @Override
  public float readFloat() throws IOException {
    advance(Symbol.FLOAT);
    if (in.getCurrentToken().isNumeric()) {
      float result = in.getFloatValue();
      in.nextToken();
      return result;
    } else {
      throw error("float");
    }
  }

  @Override
  public double readDouble() throws IOException {
    advance(Symbol.DOUBLE);
    JsonToken currentToken = in.getCurrentToken();
    double result;
    if (currentToken.isNumeric()) {
      result = in.getDoubleValue();
    } else if (currentToken == JsonToken.VALUE_STRING) {
      result = Double.parseDouble(in.getText());
    } else {
      throw error("double");
    }
    in.nextToken();
    return result;
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    return new Utf8(readString());
  }

  @Override
  public String readString() throws IOException {
    advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      if (in.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw error("map-key");
      }
    } else {
      if (in.getCurrentToken() != JsonToken.VALUE_STRING) {
        throw error("string");
      }
    }
    String result = in.getText();
    in.nextToken();
    return result;
  }

  @Override
  public void skipString() throws IOException {
    advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      if (in.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw error("map-key");
      }
    } else {
      if (in.getCurrentToken() != JsonToken.VALUE_STRING) {
        throw error("string");
      }
    }
    in.nextToken();
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    advance(Symbol.BYTES);
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      byte[] result = readByteArray();
      in.nextToken();
      return ByteBuffer.wrap(result);
    } else {
      throw error("bytes");
    }
  }

  byte[] readByteArray() throws IOException {
   return in.getText().getBytes(CHARSET);
  }

  @Override
  public void skipBytes() throws IOException {
    advance(Symbol.BYTES);
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      in.nextToken();
    } else {
      throw error("bytes");
    }
  }

  private void checkFixed(int size) throws IOException {
    advance(Symbol.FIXED);
    Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
    if (size != top.size) {
      throw new AvroTypeException(
        "Incorrect length for fixed binary: expected " +
        top.size + " but received " + size + " bytes.");
    }
  }

  @Override
  public void readFixed(byte[] bytes, int start, int len) throws IOException {
    checkFixed(len);
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      byte[] result = readByteArray();
      in.nextToken();
      if (result.length != len) {
        throw new AvroTypeException("Expected fixed length " + len
            + ", but got" + result.length);
      }
      System.arraycopy(result, 0, bytes, start, len);
    } else {
      throw error("fixed");
    }
  }

  @Override
  public void skipFixed(int length) throws IOException {
    checkFixed(length);
    doSkipFixed(length);
  }

  private void doSkipFixed(int length) throws IOException {
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      byte[] result = readByteArray();
      in.nextToken();
      if (result.length != length) {
        throw new AvroTypeException("Expected fixed length " + length
            + ", but got" + result.length);
      }
    } else {
      throw error("fixed");
    }
  }

  @Override
  protected void skipFixed() throws IOException {
    advance(Symbol.FIXED);
    Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
    doSkipFixed(top.size);
  }

  @Override
  public int readEnum() throws IOException {
    advance(Symbol.ENUM);
    Symbol.EnumLabelsAction top = (Symbol.EnumLabelsAction) parser.popSymbol();
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      in.getText();
      int n = top.findLabel(in.getText());
      if (n >= 0) {
        in.nextToken();
        return n;
      }
      throw new AvroTypeException("Unknown symbol in enum " + in.getText());
    } else {
      throw error("fixed");
    }
  }

  @Override
  public long readArrayStart() throws IOException {
    advance(Symbol.ARRAY_START);
    if (in.getCurrentToken() == JsonToken.START_ARRAY) {
      in.nextToken();
      return doArrayNext();
    } else {
      throw error("array-start");
    }
  }

  @Override
  public long arrayNext() throws IOException {
    advance(Symbol.ITEM_END);
    return doArrayNext();
  }

  private long doArrayNext() throws IOException {
    if (in.getCurrentToken() == JsonToken.END_ARRAY) {
      parser.advance(Symbol.ARRAY_END);
      in.nextToken();
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public long skipArray() throws IOException {
    advance(Symbol.ARRAY_START);
    if (in.getCurrentToken() == JsonToken.START_ARRAY) {
      in.skipChildren();
      in.nextToken();
      advance(Symbol.ARRAY_END);
    } else {
      throw error("array-start");
    }
    return 0;
  }

  @Override
  public long readMapStart() throws IOException {
    advance(Symbol.MAP_START);
    if (in.getCurrentToken() == JsonToken.START_OBJECT) {
      in.nextToken();
      return doMapNext();
    } else {
      throw error("map-start");
    }
  }

  @Override
  public long mapNext() throws IOException {
    advance(Symbol.ITEM_END);
    return doMapNext();
  }

  private long doMapNext() throws IOException {
    if (in.getCurrentToken() == JsonToken.END_OBJECT) {
      in.nextToken();
      advance(Symbol.MAP_END);
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public long skipMap() throws IOException {
    advance(Symbol.MAP_START);
    if (in.getCurrentToken() == JsonToken.START_OBJECT) {
      in.skipChildren();
      in.nextToken();
      advance(Symbol.MAP_END);
    } else {
      throw error("map-start");
    }
    return 0;
  }

  @Override
  public int readIndex() throws IOException {
    advance(Symbol.UNION);
    Symbol.Alternative a = (Symbol.Alternative) parser.popSymbol();

    String label;
    if (in.getCurrentToken() == JsonToken.VALUE_NULL) {
      label = "null";
    } else if (in.getCurrentToken() == JsonToken.START_OBJECT &&
               in.nextToken() == JsonToken.FIELD_NAME) {
      label = in.getText();
      in.nextToken();
      parser.pushSymbol(Symbol.UNION_END);
    } else {
      throw error("start-union");
    }
    int n = a.findLabel(label);
    if (n < 0)
      throw new AvroTypeException("Unknown union branch " + label);
    parser.pushSymbol(a.getSymbol(n));
    return n;
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top instanceof Symbol.FieldAdjustAction) {
        Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
        String name = fa.fname;
      if (currentReorderBuffer != null) {
        List<JsonElement> node = currentReorderBuffer.savedFields.remove(name);
        if (node != null) {
          currentReorderBuffer.origParser = in;
          in = makeParser(node, in.getCodec());
          return null;
        }
      }
      if (in.getCurrentToken() == JsonToken.FIELD_NAME) {
        do {
          String fn = in.getText();
          in.nextToken();
          if (name.equals(fn) || fa.aliases.contains(fn)) {
            return null;
          } else {
            if (currentReorderBuffer == null) {
              currentReorderBuffer = new ReorderBuffer();
            }
            currentReorderBuffer.savedFields.put(fn, getValueAsTree(in, 8));
          }
        } while (in.getCurrentToken() == JsonToken.FIELD_NAME);
        throw new AvroTypeException("Expected field name not found: " + fa.fname);
      }
    } else if (top == Symbol.FIELD_END) {
      if (currentReorderBuffer != null && currentReorderBuffer.origParser != null) {
        in = currentReorderBuffer.origParser;
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
      while(in.getCurrentToken() != JsonToken.END_OBJECT){
         in.nextToken();
       }
      if (in.getCurrentToken() == JsonToken.END_OBJECT) {
        in.nextToken();
        if (top == Symbol.RECORD_END) {
          if (currentReorderBuffer != null && !currentReorderBuffer.savedFields.isEmpty()) {
            throw error("Unknown fields: " + currentReorderBuffer.savedFields.keySet());
          }
          currentReorderBuffer = reorderBuffers.pop();
        }
      } else {
        throw error(top == Symbol.RECORD_END ? "record-end" : "union-end");
      }
    } else {
      throw new AvroTypeException("Unknown action symbol " + top);
    }
    return null;
  }

  static interface JsonElement {

    JsonToken getToken();

    default String getStringValue() {
      throw new UnsupportedOperationException("for " + this);
    }

    default int getIntValue() {
      throw new UnsupportedOperationException("for " + this);
    }

    default long getLongValue() {
      throw new UnsupportedOperationException("for " + this);
    }

    default float getFloatValue() {
      throw new UnsupportedOperationException("for " + this);
    }

    default double getDoubleValue() {
      throw new UnsupportedOperationException("for " + this);
    }

    default BigDecimal getBigDecimalValue() {
      throw new UnsupportedOperationException("for " + this);
    }

    default BigInteger getBigIntegerValue() {
      throw new UnsupportedOperationException("for " + this);
    }

    default boolean getBooleanValue() {
      throw new UnsupportedOperationException("for " + this);
    }

    default JsonParser.NumberType getNumberType() {
      throw new UnsupportedOperationException("for " + this);
    }

    JsonElement TRUE = new JsonElement() {
      @Override
      public JsonToken getToken() {
        return JsonToken.VALUE_TRUE;
      }

      @Override
      public boolean getBooleanValue() {
        return true;
      }
    };

    JsonElement FALSE = new JsonElement() {
      @Override
      public JsonToken getToken() {
        return JsonToken.VALUE_FALSE;
      }

      @Override
      public boolean getBooleanValue() {
        return false;
      }
    };

    JsonElement NULL = new JsonElement() {
      @Override
      public JsonToken getToken() {
        return JsonToken.VALUE_NULL;
      }
    };

    JsonElement NONE = new JsonElement() {
      @Override
      public JsonToken getToken() {
        return null;
      }
    };


  }


  static class JsonElementIntValue  extends JsonElementToken {
    public final int value;

    public JsonElementIntValue(int value, JsonToken token) {
      super(token);
      this.value = value;
    }

    @Override
    public int getIntValue() {
     return value;
    }

    @Override
    public BigInteger getBigIntegerValue() {
      return BigInteger.valueOf(value);
    }

    @Override
    public BigDecimal getBigDecimalValue() {
      return BigDecimal.valueOf(value);
    }

    @Override
    public double getDoubleValue() {
      return (double) value;
    }

    @Override
    public float getFloatValue() {
      return (float) value;
    }

    @Override
    public long getLongValue() {
      return (long) value;
    }

    @Override
    public String getStringValue() {
      return Integer.toString(value);
    }

    @Override
    public JsonParser.NumberType getNumberType() {
      return JsonParser.NumberType.INT;
    }

  }

  static class JsonElementLongValue extends JsonElementToken {
    public final long value;

    public JsonElementLongValue(long value, JsonToken token) {
      super(token);
      this.value = value;
    }

    @Override
    public String getStringValue() {
      return Long.toString(value);
    }

    @Override
    public long getLongValue() {
     return value;
    }

    @Override
    public BigInteger getBigIntegerValue() {
      return BigInteger.valueOf(value);
    }

    @Override
    public BigDecimal getBigDecimalValue() {
      return BigDecimal.valueOf(value);
    }

    @Override
    public double getDoubleValue() {
      return (double) value;
    }

    @Override
    public float getFloatValue() {
      return (float) value;
    }

    @Override
    public JsonParser.NumberType getNumberType() {
      return JsonParser.NumberType.LONG;
    }

  }

  static class JsonElementFloatValue extends JsonElementToken {
    public final float value;

    public JsonElementFloatValue(float value, JsonToken token) {
      super(token);
      this.value = value;
    }

    @Override
    public String getStringValue() {
      return Float.toString(value);
    }

    @Override
    public float getFloatValue() {
     return value;
    }

    @Override
    public BigDecimal getBigDecimalValue() {
      return BigDecimal.valueOf(value);
    }

    @Override
    public double getDoubleValue() {
      return value;
    }

    @Override
    public JsonParser.NumberType getNumberType() {
      return JsonParser.NumberType.FLOAT;
    }

  }

  static class JsonElementDoubleValue extends JsonElementToken {
    public final double value;

    public JsonElementDoubleValue(double value, JsonToken token) {
      super(token);
      this.value = value;
    }

    @Override
    public String getStringValue() {
      return Double.toString(value);
    }

    @Override
    public double getDoubleValue() {
     return value;
    }

    @Override
    public BigDecimal getBigDecimalValue() {
      return BigDecimal.valueOf(value);
    }

    @Override
    public JsonParser.NumberType getNumberType() {
      return JsonParser.NumberType.DOUBLE;
    }

  }

  static class JsonElementBigDecimalValue extends JsonElementToken {
    public final BigDecimal value;

    public JsonElementBigDecimalValue(BigDecimal value, JsonToken token) {
      super(token);
      this.value = value;
    }

    @Override
    public String getStringValue() {
      return value.toString();
    }

    @Override
    public BigDecimal getBigDecimalValue() {
     return value;
    }

    @Override
    public double getDoubleValue() {
      return value.doubleValue();
    }

    @Override
    public float getFloatValue() {
      return value.floatValue();
    }

    @Override
    public JsonParser.NumberType getNumberType() {
      return JsonParser.NumberType.BIG_DECIMAL;
    }

  }


  static class JsonElementBigIntegerValue extends JsonElementToken {
    public final BigInteger value;

    public JsonElementBigIntegerValue(BigInteger value, JsonToken token) {
      super(token);
      this.value = value;
    }

    @Override
    public String getStringValue() {
      return value.toString();
    }

    @Override
    public BigInteger getBigIntegerValue() {
     return value;
    }

    @Override
    public double getDoubleValue() {
      return value.doubleValue();
    }

    @Override
    public float getFloatValue() {
      return value.floatValue();
    }

    @Override
    public JsonParser.NumberType getNumberType() {
      return JsonParser.NumberType.BIG_INTEGER;
    }
  }

  static class JsonElementStringValue extends JsonElementToken {
    public final String value;

    public JsonElementStringValue(JsonToken token, String value) {
      super(token);
      this.value = value;
    }

    public String getStringValue() {
      return value;
    }

  }

  static class JsonElementToken implements JsonElement {
    public final JsonToken token;

    public JsonElementToken(JsonToken token) {
      this.token = token;
    }

    public JsonToken getToken() {
      return token;
    }

    public String getStringValue() {
      return null;
    }

    @Override
    public String toString() {
      return this.getClass().getName() + "{token=" + token + ", value = " + getStringValue() + '}';
    }

  }

  static List<JsonElement> getValueAsTree(JsonParser in, final int expectedSize) throws IOException {
    int level = 0;
    List<JsonElement> result = new ArrayList<JsonElement>(expectedSize);
    do {
      JsonToken t = in.getCurrentToken();
      if (t == null) {
        break;
      }
      switch (t) {
      case START_OBJECT:
      case START_ARRAY:
        level++;
        result.add(new JsonElementToken(t));
        break;
      case END_OBJECT:
      case END_ARRAY:
        level--;
        result.add(new JsonElementToken(t));
        break;
      case FIELD_NAME:
      case VALUE_STRING:
        result.add(new JsonElementStringValue(t, in.getText()));
        break;
      case VALUE_NUMBER_INT:
        JsonParser.NumberType numberType = in.getNumberType();
        switch (numberType) {
          case BIG_INTEGER:
            result.add(new JsonElementBigIntegerValue(in.getBigIntegerValue(), t));
            break;
          case INT:
            result.add(new JsonElementIntValue(in.getIntValue(), t));
            break;
          case LONG:
            result.add(new JsonElementLongValue(in.getLongValue(), t));
            break;
          default:
            throw new UnsupportedOperationException("Unsupperted int number type " + numberType);
        }
        break;
      case VALUE_NUMBER_FLOAT:
        numberType = in.getNumberType();
        switch (numberType) {
          case FLOAT:
          case DOUBLE:
          case BIG_DECIMAL:
            result.add(new JsonElementBigDecimalValue(in.getDecimalValue(), t));
            break;
          default:
            throw new UnsupportedOperationException("Unsupperted int number type " + numberType);
        }
        break;
      case VALUE_TRUE:
        result.add(JsonElement.TRUE);
        break;
      case VALUE_FALSE:
        result.add(JsonElement.FALSE);
        break;
      case VALUE_NULL:
        result.add(JsonElement.NULL);
        break;
      }
      in.nextToken();
    } while (level != 0);
    result.add(JsonElement.NONE);
    return result;
  }

  public static JsonParser makeParser(final List<JsonElement> elements, final ObjectCodec codec) throws IOException {
    return new JsonParser() {
      int pos = 0;

      private JsonElement currElement = elements.get(pos);

      @Override
      public ObjectCodec getCodec() {
        return codec;
      }

      @Override
      public void setCodec(ObjectCodec c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public JsonToken nextToken() throws IOException {
        pos++;
        if (pos < elements.size()) {
          currElement = elements.get(pos);
          return currElement.getToken();
        } else {
          currElement = JsonElement.NONE;
          return null;
        }
      }

      @Override
      public JsonParser skipChildren() throws IOException {
        JsonToken tkn = currElement.getToken();
        int level = (tkn == JsonToken.START_ARRAY || tkn == JsonToken.START_OBJECT) ? 1 : 0;
        while (level > 0) {
          switch(elements.get(++pos).getToken()) {
          case START_ARRAY:
          case START_OBJECT:
            level++;
            break;
          case END_ARRAY:
          case END_OBJECT:
            level--;
            break;
          }
        }
        currElement = elements.get(pos);
        return this;
      }

      @Override
      public boolean isClosed() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getCurrentName() throws IOException {
        return currElement.getStringValue();
      }

      @Override
      public JsonStreamContext getParsingContext() {
        throw new UnsupportedOperationException();
      }

      @Override
      public JsonLocation getTokenLocation() {
        throw new UnsupportedOperationException();
      }

      @Override
      public JsonLocation getCurrentLocation() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getText() throws IOException {
        return currElement.getStringValue();
      }

      @Override
      public char[] getTextCharacters() throws IOException {
        return currElement.getStringValue().toCharArray();
      }

      @Override
      public int getTextLength() throws IOException {
        return currElement.getStringValue().length();
      }

      @Override
      public int getTextOffset() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public Number getNumberValue() throws IOException {
        JsonParser.NumberType numberType = currElement.getNumberType();
        switch(numberType) {
          case BIG_DECIMAL:
            return currElement.getBigDecimalValue();
          case BIG_INTEGER:
            return currElement.getBigDecimalValue();
          case DOUBLE:
            return currElement.getDoubleValue();
          case FLOAT:
            return currElement.getFloatValue();
          case INT:
            return currElement.getIntValue();
          case LONG:
            return currElement.getLongValue();
          default:
            throw new UnsupportedOperationException("Unsupported number type: " + numberType);
        }
      }

      @Override
      public NumberType getNumberType() throws IOException {
        return currElement.getNumberType();
      }

      @Override
      public int getIntValue() throws IOException {
        return currElement.getIntValue();
      }

      @Override
      public long getLongValue() throws IOException {
        return currElement.getLongValue();
      }

      @Override
      public BigInteger getBigIntegerValue() throws IOException {
        return currElement.getBigIntegerValue();
      }

      @Override
      public float getFloatValue() throws IOException {
        return currElement.getFloatValue();
      }

      @Override
      public double getDoubleValue() throws IOException {
        return currElement.getDoubleValue();
      }

      @Override
      public BigDecimal getDecimalValue() throws IOException {
        return currElement.getBigDecimalValue();
      }

      @Override
      public byte[] getBinaryValue(Base64Variant b64variant)
        throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public JsonToken getCurrentToken() {
        return currElement.getToken();
      }
    };
  }

  AvroTypeException error(String type) {
    return new AvroTypeException("Expected " + type +
        ". Got " + in.getCurrentToken());
  }

}

