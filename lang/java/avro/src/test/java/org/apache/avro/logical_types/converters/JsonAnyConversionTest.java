
package org.apache.avro.logical_types.converters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DirectBinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.logical_types.JsonAny;
import org.junit.Test;


/**
 *
 * @author Zoltan Farkas
 */
 public class JsonAnyConversionTest {

  @Test
  public void testBackAndForth() {
    JsonAnyConversion conv = new JsonAnyConversion();
    Schema schema = Schema.create(Schema.Type.STRING);
    JsonAny lt = JsonAny.instance();
    lt.addToSchema(schema);
    CharSequence cs = conv.toCharSequence("127.0.0.1", schema, lt);
    String back = (String) conv.fromCharSequence(cs, schema, lt);
    Assert.assertEquals("127.0.0.1", back);
  }

  @Test
  public void testBackAndForthSer() throws IOException {
    Schema schema = Schema.create(Schema.Type.STRING);
    JsonAny lt = JsonAny.instance();
    lt.addToSchema(schema);
    Schema maoSchema = SchemaBuilder.map().values(schema);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder enc =  EncoderFactory.get().binaryEncoder(bos, null);
    DatumWriter dw =  GenericData.get().createDatumWriter(maoSchema);
    Map<String, String> data = new HashMap<>();
    data.put("bla", "127.0.0.1");
    dw.write(data, enc);
    enc.flush();
    ByteArrayInputStream bin = new ByteArrayInputStream(bos.toByteArray());
    BinaryDecoder bd =DecoderFactory.get().binaryDecoder(bin, null);
    DatumReader dr = GenericData.get().createDatumReader(maoSchema);
    Map<String, String> res =  (Map) dr.read(null, bd);
    Assert.assertEquals(data, res);
  }

}
