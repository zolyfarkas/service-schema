package org.apache.avro;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.concurrent.Callable;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;
import org.junit.Assert;
import org.junit.Test;

public class TestLogicalType {

  @Test
  public void testDecimalFromJsonNode() {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("logicalType", TextNode.valueOf("decimal"));
    node.put("precision", IntNode.valueOf(9));
    node.put("scale", IntNode.valueOf(2));
    LogicalType decimal = LogicalTypes.fromJsonNode(node, Schema.Type.STRING);
    Assert.assertNotNull("Should be a Decimal", decimal);
    Assert.assertEquals("Should have correct precision",
        9, decimal.getProperty("precision"));
    Assert.assertEquals("Should have correct scale",
        2, decimal.getProperty("scale"));
  }

  @Test
  public void testDecimalWithNonByteArrayOrStringTypes() {
    // test simple types
    Schema[] nonBytes = new Schema[] {
        Schema.createRecord("Record", null, null, false),
        Schema.createArray(Schema.create(Schema.Type.BYTES)),
        Schema.createMap(Schema.create(Schema.Type.BYTES)),
        Schema.createEnum("Enum", null, null, Arrays.asList("a", "b")),
        Schema.createUnion(Arrays.asList(
            Schema.create(Schema.Type.BYTES),
            Schema.createFixed("fixed", null, null, 4))),
        Schema.create(Schema.Type.BOOLEAN), Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.FLOAT),
        Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL)};
    for (final Schema schema : nonBytes) {
      try {
       LogicalTypes.create(schema.getType(), ImmutableMap.of("logicalType", "decimal"));
       Assert.fail("should not be able to create " + schema);
      } catch (IllegalArgumentException ex) {
        // expected
      }

    }
  }

  @Test(expected = RuntimeException.class)
  public void testUnknownFromJsonNode() {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("logicalType", TextNode.valueOf("unknown"));
    node.put("someProperty", IntNode.valueOf(34));
    LogicalType logicalType = LogicalTypes.fromJsonNode(node, Schema.Type.STRING);
    Assert.assertNull("Should not return a LogicalType instance", logicalType);
  }


  public static void assertEqualsTrue(String message, Object o1, Object o2) {
    Assert.assertTrue("Should be equal (forward): " + message, o1.equals(o2));
    Assert.assertTrue("Should be equal (reverse): " + message, o2.equals(o1));
  }

  public static void assertEqualsFalse(String message, Object o1, Object o2) {
    Assert.assertFalse("Should be equal (forward): " + message, o1.equals(o2));
    Assert.assertFalse("Should be equal (reverse): " + message, o2.equals(o1));
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param containedInMessage A String that should be contained by the thrown
   *                           exception's message
   * @param callable A Callable that is expected to throw the exception
   */
  public static void assertThrows(String message,
                                  Class<? extends Exception> expected,
                                  String containedInMessage,
                                  Callable callable) {
    try {
      callable.call();
      Assert.fail("No exception was thrown (" + message + "), expected: " +
          expected.getName());
    } catch (Exception actual) {
      Assert.assertEquals(message, expected, actual.getClass());
      Assert.assertTrue(
          "Expected exception message (" + containedInMessage + ") missing: " +
              actual.getMessage(),
          actual.getMessage().contains(containedInMessage)
      );
    }
  }
}
