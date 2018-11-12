/*
 * Copyright 2015 The Apache Software Foundation.
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
package org.apache.avro.compiler.idl;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypeFactory;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author zoly
 */
public class TestProtocol {

    @Test
    public void test() throws ParseException, IOException {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Idl idl = new Idl(cl.getResourceAsStream("test/simple.avdl"),
                "UTF-8");
        Protocol protocol = idl.CompilationUnit();
        String json = protocol.toString(true);
        System.out.println(json);
    }

  @Test
  public void testToString() throws ParseException, MalformedURLException, IOException {
    File file = new File(".");
    String currentWorkPath = file.getAbsolutePath();
    String testIdl = currentWorkPath + File.separator + "src" + File.separator + "test"
        + File.separator + "idl" + File.separator + File.separator + "test/test.avdl";
    Idl idl = new Idl(new File(testIdl));
    idl.setIsAllowUndefinedLogicalTypes(true);
    Protocol protocol = idl.CompilationUnit();
    int i = 0;
    for (Schema s : protocol.getTypes()) {
      s.addProp("id", "A" + i);
    }

    SpecificCompiler compiler = new SpecificCompiler(protocol);
    compiler.setStringType(GenericData.StringType.String);
    compiler.setFieldVisibility(SpecificCompiler.FieldVisibility.PRIVATE);
    compiler.setCreateSetters(true);
    File dest = new File("./target/tp1");
    dest.mkdirs();
    compiler.compileToDestination(null, dest);

    String strProto = protocol.toString(true);
    System.out.println(strProto);
    Protocol protocol2 = Protocol.parse(strProto, true);
    Assert.assertEquals(protocol, protocol2);
  }

  @Test
  public void testToString2() throws ParseException, MalformedURLException, IOException {
    LogicalTypes.register(new LogicalTypeFactory() {
      @Override
      public String getLogicalTypeName() {
        return "date";
      }

      @Override
      public LogicalType create(Schema.Type schemaType, Map<String, Object> attributes) {
        return new LogicalType() {
          @Override
          public String getName() {
            return "date";
          }

          @Override
          public Set reserved() {
            throw new UnsupportedOperationException();
          }

          @Override
          public Object getProperty(String propertyName) {
           return null;
          }

          @Override
          public Map getProperties() {
            return Collections.EMPTY_MAP;
          }

          @Override
          public Class getLogicalJavaType() {
            return LocalDate.class;
          }

          @Override
          public Object deserialize(Object object) {
            throw new UnsupportedOperationException();
          }

          @Override
          public Object serialize(Object object) {
            throw new UnsupportedOperationException();
          }
        };
      }
    });


    File file = new File(".");
    String currentWorkPath = file.getAbsolutePath();
    String testIdl = currentWorkPath + File.separator + "src" + File.separator + "test"
        + File.separator + "idl" + File.separator + File.separator + "test/test.avdl";
    Idl idl = new Idl(new File(testIdl));
    idl.setIsAllowUndefinedLogicalTypes(false);
    Protocol protocol = idl.CompilationUnit();
    int i = 0;
    for (Schema s : protocol.getTypes()) {
      s.addProp("id", "A" + i);
    }

    SpecificCompiler compiler = new SpecificCompiler(protocol);
    compiler.setStringType(GenericData.StringType.String);
    compiler.setFieldVisibility(SpecificCompiler.FieldVisibility.PRIVATE);
    compiler.setCreateSetters(true);
    File dest = new File("./target/tp2");
    dest.mkdirs();
    compiler.compileToDestination(null, dest);

    String strProto = protocol.toString(true);
    System.out.println(strProto);
    Protocol protocol2 = Protocol.parse(strProto, false);
    Assert.assertEquals(protocol, protocol2);
    LogicalTypes.unregister("date");
  }



}
