package org.apache.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.ExtendedJsonEncoder;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import static org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE;

/**
 * Extension of the AVRO GenericDatumWriter that does the following extras over the base class
 * If the value being written equals the schema default we dont write the field or value at all!
 * We also encode unions of null and a singletype as a more normal key=value rather than key={type=value}.
 *
 * @author zfarkas
 */
public class ExtendedGenericDatumWriter<D> extends GenericDatumWriter<D> {

    public ExtendedGenericDatumWriter(final GenericData data) {
        super(data);
    }

    public ExtendedGenericDatumWriter(final Schema root) {
        super(root);
    }

    public ExtendedGenericDatumWriter(final Schema root, final GenericData data) {
        super(root, data);
    }

   public static boolean equals(Object a, Object defaultValue) {
     if (a == defaultValue) {
       return true;
     }
     if (NULL_DEFAULT_VALUE == defaultValue && a == null) {
       return true;
     }
     boolean result = a != null && a.equals(defaultValue);
     if (result) {
       return true;
     }
     if ((a instanceof Enum || a instanceof GenericEnumSymbol) && defaultValue != null) {
       return a.toString().equals(defaultValue.toString());
     } else if (a instanceof CharSequence && defaultValue instanceof CharSequence) {
       return equals((CharSequence) a, (CharSequence) defaultValue);
     } else {
       return false;
     }

   }

   public static boolean equals(@Nonnull final CharSequence s, @Nonnull final CharSequence t) {
    final int sl = s.length();
    final int tl = t.length();
    if (sl != tl) {
      return false;
    } else {
      for (int i = 0; i < sl; i++) {
        if (s.charAt(i) != t.charAt(i)) {
          return false;
        }
      }
      return true;
    }
  }


   private static final ThreadLocal<List<Symbol>> HOLDINGS = new ThreadLocal<List<Symbol>>() {

      @Override
      protected List<Symbol> initialValue() {
        return new ArrayList(8);
      }

   };

    /**
     * Overwritten to skip serializing fields that have default values.
     *
     * @param datum
     * @param f
     * @param out
     * @param state
     * @throws IOException
     */
    //CHECKSTYLE IGNORE DesignForExtension FOR NEXT 30 LINES
    @Override
    protected void writeField(final Object datum, final Schema.Field f, final Encoder out, final Object state)
            throws IOException {
        GenericData data = getData();
        Object defaultValue = f.defaultVal();
        if (defaultValue != null) {
            Object value = data.getField(datum, f.name(), f.pos());

            if (equals(value, defaultValue) && out instanceof ExtendedJsonEncoder) {
                    Parser parser = ((ExtendedJsonEncoder) out).getParser();
                    Symbol topSymbol = parser.topSymbol();
                    switch (topSymbol.kind) {
                        case TERMINAL:
                        case IMPLICIT_ACTION:
                            break;
                        default:
                            // expand production
                            final Symbol nextSymbol = topSymbol.production[topSymbol.production.length - 1];
                            if (nextSymbol instanceof Symbol.ImplicitAction) {
                                if (((Symbol.ImplicitAction) nextSymbol).isTrailing) {
                                    throw new IllegalStateException("Cannot start with a trailing implicit"
                                            + topSymbol);
                                } else {
                                    parser.advance(nextSymbol);
                                }
                            } else {
                                throw new IllegalStateException("Invalid state " + topSymbol);
                            }
                            parser.pushSymbol(nextSymbol);
                    }
                    List<Symbol> holdings = HOLDINGS.get();
                    holdings.clear();
                    Symbol advanceTo = null;
                    boolean done = false;
                    while (!done) {
                        if (parser.depth() > 0) {
                          advanceTo = parser.popSymbol();
                          if (advanceTo instanceof Symbol.FieldAdjustAction
                                  && ((Symbol.FieldAdjustAction) advanceTo).fname.equals(f.name())
                                  && ((Symbol.FieldAdjustAction) advanceTo).rindex == f.pos()) {
                              done = true;
                          }
                          holdings.add(advanceTo);
                        } else {
                          done = true;
                        }
                    }
                    for (int i = holdings.size() - 1; i >= 0; i--) {
                        parser.pushSymbol(holdings.get(i));
                    }
                    parser.advance(advanceTo);
                    int count = 1;
                    while (count > 0) {
                        Symbol currentSymbol = parser.popSymbol();
                        if (currentSymbol.getClass() == Symbol.ImplicitAction.class) {
                            if (((Symbol.ImplicitAction) currentSymbol).isTrailing) {
                                count--;
                            } else {
                                count++;
                            }
                        }
                    }
            } else {
                super.writeField(datum, f, out, state);
            }
        } else {
            super.writeField(datum, f, out, state);
        }

    }

}

