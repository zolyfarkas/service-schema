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
package org.apache.avro.compiler.specific;

import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.AvroTestUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.StringType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.apache.avro.Protocol;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;

@RunWith(JUnit4.class)
public class TestSpecificCompiler2 {
  private final String schemaSrcPath = "src/test/idl/input/union.avdl";
  private final String velocityTemplateDir =
      "src/main/velocity/org/apache/avro/compiler/specific/templates/java/classic/";
  private File src;
  private File outputDir;
  private File outputFile;

  @Before
  public void setUp() {
    this.src = new File(this.schemaSrcPath);
    this.outputDir = new File("target/specificOutput");
    this.outputDir.mkdirs();
    this.outputFile = new File(this.outputDir + "/org/apache/avro/gen", "SR2.java");
    this.outputFile.delete();
  }


  /** Uses the system's java compiler to actually compile the generated code. */
  static void assertCompilesWithJavaCompiler(Collection<SpecificCompiler.OutputFile> outputs)
          throws IOException {
    if (outputs.isEmpty())
      return;               // Nothing to compile!

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager fileManager =
            compiler.getStandardFileManager(null, null, null);

    File dstDir = AvroTestUtil.tempFile(TestSpecificCompiler2.class, "realCompiler");
    List<File> javaFiles = new ArrayList<File>();
    for (SpecificCompiler.OutputFile o : outputs) {
      javaFiles.add(o.writeToDestination(null, dstDir));
    }

    JavaCompiler.CompilationTask cTask = compiler.getTask(null, fileManager,
            null, null, null, fileManager.getJavaFileObjects(
                    javaFiles.toArray(new File[javaFiles.size()])));
    boolean compilesWithoutError = cTask.call();
    assertTrue(compilesWithoutError);
  }



  private SpecificCompiler createCompiler() throws IOException, ParseException {
    Idl idl = new Idl(src);
    Protocol protocol = idl.CompilationUnit();
    Schema schema = protocol.getType("org.apache.avro.gen.SR2");
    SpecificCompiler compiler = new SpecificCompiler(schema);
    compiler.setTemplateDir(this.velocityTemplateDir);
    compiler.setStringType(StringType.CharSequence);
    return compiler;
  }

  @Test
  public void testCanReadTemplateFilesOnTheFilesystem() throws IOException, URISyntaxException, ParseException{
    SpecificCompiler compiler = createCompiler();
    compiler.compileToDestination(this.src, this.outputDir);
    assertTrue(this.outputFile.exists());
  }


}
