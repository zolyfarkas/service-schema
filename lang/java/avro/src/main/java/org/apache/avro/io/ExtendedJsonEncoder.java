package org.apache.avro.io;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.avro.AvroTypeException;
import org.apache.avro.io.parsing.JsonGrammarGenerator;

/**
 * A derived encoder that does the skipping of fields that match the index. It also encodes unions of null and a single
 * type as a more normal key=value rather than key={type=value}.
 * @author zfarkas
 */
public final class ExtendedJsonEncoder extends JsonEncoder
        implements JsonExtensionEncoder {


  public ExtendedJsonEncoder(final Schema sc, final OutputStream out) throws IOException {
      super(sc, out);
  }

  public ExtendedJsonEncoder(final Schema sc, final OutputStream out, final boolean pretty) throws IOException {
    super(sc, out, pretty);
  }

  public ExtendedJsonEncoder(final Schema sc, final JsonGenerator out) throws IOException {
    super(sc, out);
  }

  public Parser getParser() {
    return parser;
  }

  public static boolean isNullableSingle(final Symbol.Alternative top) {
    return top.size() == 2 && ("null".equals(top.getLabel(0)) || "null".equals(top.getLabel(1)));
  }

  public static String getNullableSingle(final Symbol.Alternative top) {
    final String label = top.getLabel(0);
    return "null".equals(label) ? top.getLabel(1) : label;
  }

  /**
   * Overwrite this function to optime json decoding of union {null, type}.
   * @param unionIndex
   * @throws IOException
   */

  @Override
  public void writeIndex(final int unionIndex) throws IOException {
    parser.advance(Symbol.UNION);
    Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
    Symbol symbol = top.getSymbol(unionIndex);
    if (symbol != Symbol.NULL && !isNullableSingle(top)) {
      out.writeStartObject();
      out.writeFieldName(top.getLabel(unionIndex));
      parser.pushSymbol(Symbol.UNION_END);
    }
    parser.pushSymbol(symbol);
  }

  @Override
  public void writeDecimal(final BigDecimal decimal, final Schema schema) throws IOException {
    advanceBy(schema);
    out.writeNumber(decimal);
  }

  @Override
  public void writeBigInteger(BigInteger decimal,final Schema schema) throws IOException {
    advanceBy(schema);
    out.writeNumber(decimal);
  }

  @Override
  public void writeValue(final Object value, final Schema schema) throws IOException {
    if (value == null) {
      throw new AvroTypeException("value cannot be null, must be " + schema);
    }
    advanceBy(schema);
    out.writeObject(value);
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
          Symbol rootSymbol = JsonGrammarGenerator.getRootSymbol(schema);
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

}
