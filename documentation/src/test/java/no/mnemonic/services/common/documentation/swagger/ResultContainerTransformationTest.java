package no.mnemonic.services.common.documentation.swagger;

import io.swagger.models.*;
import io.swagger.models.properties.ArrayProperty;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;
import no.mnemonic.commons.utilities.collections.MapUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class ResultContainerTransformationTest {

  private final ResultContainerTransformation transformation = new ResultContainerTransformation(ResultContainer.class, "data");

  @Test
  public void testBeforeHookInjectsResultContainerModel() {
    Swagger swagger = new Swagger();

    transformation.beforeHook(swagger);

    assertFalse(MapUtils.isEmpty(swagger.getDefinitions()));
    assertNotNull(swagger.getDefinitions().get("ResultContainer"));
  }

  @Test
  public void testTransformOperationForSingleObject() {
    Operation operation = createOperation(createRefProperty());
    Swagger swagger = createSwagger(operation);

    transformation.beforeHook(swagger);
    transformation.transformOperation(swagger, operation);

    // Test that the schema points to the injected ResultContainer model definition.
    RefProperty resultContainerRefProperty = (RefProperty) swagger.getPath("/test").getGet().getResponses().get("200").getSchema();
    assertEquals("ResultContainer-TestObject-single", resultContainerRefProperty.getSimpleRef());

    // Test that the injected ResultContainer model definition exists.
    Model refModelDefinition = swagger.getDefinitions().get("ResultContainer-TestObject-single");
    assertNotNull(refModelDefinition);

    // Test that the 'data' property points to the correct model definition.
    RefProperty dataRefProperty = (RefProperty) refModelDefinition.getProperties().get("data");
    assertEquals("TestObject", dataRefProperty.getSimpleRef());
  }

  @Test
  public void testTransformOperationForObjectList() {
    Operation operation = createOperation(createArrayProperty());
    Swagger swagger = createSwagger(operation);

    transformation.beforeHook(swagger);
    transformation.transformOperation(swagger, operation);

    // Test that the schema points to the injected ResultContainer model definition.
    RefProperty resultContainerRefProperty = (RefProperty) swagger.getPath("/test").getGet().getResponses().get("200").getSchema();
    assertEquals("ResultContainer-TestObject-list", resultContainerRefProperty.getSimpleRef());

    // Test that the injected ResultContainer model definition exists.
    Model refModelDefinition = swagger.getDefinitions().get("ResultContainer-TestObject-list");
    assertNotNull(refModelDefinition);

    // Test that the 'data' property points to the correct model definition.
    ArrayProperty dataArrayProperty = (ArrayProperty) refModelDefinition.getProperties().get("data");
    assertEquals("TestObject", ((RefProperty) dataArrayProperty.getItems()).getSimpleRef());
  }

  private Swagger createSwagger(Operation operation) {
    ModelImpl model = new ModelImpl();
    model.setName("TestObject");

    Path path = new Path();
    path.setGet(operation);

    Swagger swagger = new Swagger();
    swagger.path("/test", path);
    swagger.addDefinition("TestObject", model);

    return swagger;
  }

  private Operation createOperation(Property schema) {
    Response response = new Response();
    response.schema(schema);

    Operation operation = new Operation();
    operation.addResponse("200", response);

    return operation;
  }

  private Property createRefProperty() {
    RefProperty property = new RefProperty();
    property.set$ref("#/definitions/TestObject");

    return property;
  }

  private Property createArrayProperty() {
    ArrayProperty property = new ArrayProperty();
    property.setItems(createRefProperty());

    return property;
  }

  private static class ResultContainer {
    private Object data;

    public Object getData() {
      return data;
    }

    public ResultContainer setData(Object data) {
      this.data = data;
      return this;
    }
  }

}
