/*
 * Copyright 2017 The Apache Software Foundation.
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

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.junit.Assert;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class ExtendedJsonDecoderTest {


  @Test
  public void testDecoding() throws IOException {
    String data = Resources.toString(Resources.getResource("testData.json"), Charsets.UTF_8);
    String writerSchemaStr = Resources.toString(Resources.getResource("testDataWriterSchema.json"), Charsets.UTF_8);
    String readerSchemaStr = Resources.toString(Resources.getResource("testDataReaderSchema.json"), Charsets.UTF_8);
    Schema writerSchema = new Schema.Parser().parse(writerSchemaStr);
    Schema readerSchema = new Schema.Parser().parse(readerSchemaStr);
    ByteArrayInputStream bis = new ByteArrayInputStream(data.getBytes(Charsets.UTF_8));
    ExtendedJsonDecoder decoder = new ExtendedJsonDecoder(writerSchema, bis);
    GenericDatumReader reader = new GenericDatumReader(writerSchema, readerSchema);
    GenericRecord testData = (GenericRecord) reader.read(null, decoder);
    Assert.assertEquals(Long.valueOf(1L), ((Map<String, Long>) testData.get("someMap")).get("A"));
    Assert.assertEquals("caca", testData.get("someField").toString());
  }

  @Test
  public void testDoubleHandling() throws IOException {
    Schema recordSchema = SchemaBuilder.record("TestRecord").fields()
            .name("doubleVal").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
            .name("defDoubleVal").type(Schema.create(Schema.Type.DOUBLE)).withDefault(Double.NaN)
            .endRecord();

    GenericData.Record record = new GenericData.Record(recordSchema);
    record.put("doubleVal", Double.NaN);
    record.put("defDoubleVal", Double.NaN);
    Assert.assertTrue(Double.isNaN(serDeser(record)));

    record = new GenericData.Record(recordSchema);
    record.put("doubleVal", Double.POSITIVE_INFINITY);
    record.put("defDoubleVal", Double.NaN);
    double serDeser = serDeser(record);
    Assert.assertTrue(Double.isInfinite(serDeser));
    Assert.assertTrue(serDeser > 0);

    record = new GenericData.Record(recordSchema);
    record.put("doubleVal", Double.NEGATIVE_INFINITY);
    record.put("defDoubleVal", Double.NaN);    
    serDeser = serDeser(record);
    Assert.assertTrue(Double.isInfinite(serDeser));
    Assert.assertTrue(serDeser < 0);
  }

  public double  serDeser(GenericData.Record record) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Schema schema = record.getSchema();
    ExtendedJsonEncoder encoder = new ExtendedJsonEncoder(schema, bos);
    GenericDatumWriter writer = new GenericDatumWriter(schema);
    writer.write(record, encoder);
    encoder.flush();
    System.out.println(bos.toString());
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    ExtendedJsonDecoder decoder = new ExtendedJsonDecoder(schema, bis);
    GenericDatumReader reader = new GenericDatumReader(schema, schema);
    GenericRecord testData = (GenericRecord) reader.read(null, decoder);
    return (double) testData.get("doubleVal");
  }


}
