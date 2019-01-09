package org.apache.avro.reflect;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.Assert;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.logicalTypes.TestAnyLogicalType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Zoltan Farkas
 */
public class ExtendedReflectDataTest {

  private static final Logger LOG = LoggerFactory.getLogger(ExtendedReflectDataTest.class);

  public <T> List<T> testMethod(Class<T> clasz) {
    return null;
  }

  public <T> List<GenericRecord> testMethod2(Class<T> clasz) {
    return null;
  }

  @Test
  public void testParameterizedTypes() throws NoSuchMethodException {
    ExtendedReflectData rdata = new ExtendedReflectData();
    Method m = ExtendedReflectDataTest.class.getMethod("testMethod", new Class[] {Class.class});
    Type rt = m.getGenericReturnType();
    Schema createSchema = rdata.getSchema(rt, "T", String.class);
    LOG.debug("schema", createSchema);
  }

  @Test
  public void testParameterizedTypes2() throws NoSuchMethodException {
    ExtendedReflectData rdata = new ExtendedReflectData();
    Method m = ExtendedReflectDataTest.class.getMethod("testMethod2", new Class[] {Class.class});
    Type rt = m.getGenericReturnType();

    Schema createSchema = rdata.createSchema(rt, Arrays.asList(TestAnyLogicalType.createTestRecord()), new HashMap<>());
    LOG.debug("schema", createSchema);
  }

  @Test
  public void testParameterizedTypes3() throws NoSuchMethodException {
    ExtendedReflectData rdata = new ExtendedReflectData();

    Schema createSchema = rdata.createSchema(List.class,
            Arrays.asList(ImmutableMap.of("a", TestAnyLogicalType.createTestRecord())), new HashMap<>());
    System.out.println(createSchema);
    Assert.assertEquals("{\"type\":\"array\",\"items\":{\"type\":\"map\",\"values\":"
            + "{\"type\":\"record\",\"name\":\"test_record\",\"fields\":[{\"name\":\"someCrap\","
            + "\"type\":\"boolean\",\"default\":true},{\"name\":\"anyField\",\"type\":{\"type\":"
            + "\"record\",\"name\":\"test\",\"fields\":[{\"name\":\"avsc\",\"type\":\"string\"},"
            + "{\"name\":\"content\",\"type\":\"bytes\"}],\"logicalType\":\"any\"}},{\"name\":\"otherCrap\","
            + "\"type\":\"string\",\"default\":\"bubu\"}]}}}",
            createSchema.toString());
  }

}
