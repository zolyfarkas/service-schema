/*
 * Copyright 2019 The Apache Software Foundation.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import org.apache.avro.AvroUtils;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Zoltan Farkas
 */
public class TestSchemaLogicalType {


  @Test
  public void testJsonRecord() throws IOException {
    Schema strType = Schema.create(Schema.Type.STRING);
    strType.addProp(LogicalType.LOGICAL_TYPE_PROP, "avsc");
    LogicalType lt = LogicalTypes.fromSchema(strType);
    strType.setLogicalType(lt);

    Schema testSchema = SchemaBuilder.builder().record("test_record").fields()
            .name("avsc").type(strType).noDefault()
            .requiredInt("ifield")
            .endRecord();
    GenericData.Record record = new GenericData.Record(testSchema);
    record.put("avsc", Schema.create(Schema.Type.DOUBLE));
    record.put("ifield", 5);
    String writeAvroExtendedJson = AvroUtils.writeAvroExtendedJson(record);
    System.out.println(writeAvroExtendedJson);
    GenericRecord back = AvroUtils.readAvroExtendedJson(new StringReader(writeAvroExtendedJson), testSchema);
    Assert.assertEquals(record.toString(), back.toString());
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    AvroUtils.writeAvroBin(bos,  record);
    System.out.println(new String(bos.toByteArray(), StandardCharsets.UTF_8));
    back = (GenericRecord) AvroUtils.readAvroBin(new ByteArrayInputStream(bos.toByteArray()),
            record.getSchema());
    Assert.assertEquals(record.toString(), back.toString());

  }

}
