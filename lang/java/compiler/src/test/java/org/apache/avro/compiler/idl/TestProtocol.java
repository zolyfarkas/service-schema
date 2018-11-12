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
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
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
    Idl compiler = new Idl(new File(testIdl));
    compiler.setIsAllowUndefinedLogicalTypes(true);
    Protocol protocol = compiler.CompilationUnit();
    int i = 0;
    for (Schema s : protocol.getTypes()) {
      s.addProp("id", "A" + i);
    }


    String strProto = protocol.toString(true);
    System.out.println(strProto);
    Protocol protocol2 = Protocol.parse(strProto, true);
    Assert.assertEquals(protocol, protocol2);
  }

}
