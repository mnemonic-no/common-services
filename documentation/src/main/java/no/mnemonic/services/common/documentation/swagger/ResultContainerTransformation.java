package no.mnemonic.services.common.documentation.swagger;

import io.swagger.converter.ModelConverters;
import io.swagger.models.*;
import io.swagger.models.properties.ArrayProperty;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;
import io.swagger.models.refs.RefFormat;
import io.swagger.util.Json;
import no.mnemonic.commons.utilities.collections.MapUtils;

import java.util.Map;

/**
 * Transformation which wraps the response model of an API operation into a container model. Useful when an API
 * operation documents a response model which is different from the actually returned model, e.g. when each API
 * operation returns a container wrapping the actual result.
 */
public class ResultContainerTransformation implements SwaggerModelTransformation {

  private final Class resultContainerClass;
  private final String resultContainerName;
  private final String resultContainerDataProperty;

  public ResultContainerTransformation(Class resultContainerClass, String resultContainerDataProperty) {
    this.resultContainerClass = resultContainerClass;
    this.resultContainerName = resultContainerClass.getSimpleName();
    this.resultContainerDataProperty = resultContainerDataProperty;
  }

  @Override
  public void beforeHook(Swagger swagger) {
    // Read result container and inject its model definition into Swagger.
    Map<String, Model> resultContainerModels = ModelConverters.getInstance().readAll(Json.mapper().constructType(resultContainerClass));
    swagger.setDefinitions(MapUtils.concatenate(swagger.getDefinitions(), resultContainerModels));
  }

  @Override
  public void transformOperation(Swagger swagger, Operation operation) {
    for (Response response : MapUtils.map(operation.getResponses()).values()) {
      if (response == null || response.getSchema() == null) {
        continue;
      }
      response.setSchema(evaluateSchema(swagger, response.getSchema()));
    }
  }

  private Property evaluateSchema(Swagger swagger, Property schema) {
    Property newSchemaModel = null;
    if (schema instanceof RefProperty) {
      // Only response type defined in @ApiOperation:
      // e.g. response = JavaBean.class
      newSchemaModel = wrapWithResultContainerModel(swagger, (RefProperty) schema, SwaggerRefPropertyFactory.PropertyContainerType.NONE);
    } else if (schema instanceof ArrayProperty) {
      // Both response type and container "list" defined in @ApiOperation:
      // e.g. response = JavaBean.class, responseContainer = list
      Property itemsProperty = ArrayProperty.class.cast(schema).getItems();
      if (itemsProperty instanceof RefProperty) {
        newSchemaModel = wrapWithResultContainerModel(swagger, (RefProperty) itemsProperty, SwaggerRefPropertyFactory.PropertyContainerType.LIST);
      }
    }

    return newSchemaModel != null ? newSchemaModel : schema;
  }

  private Property wrapWithResultContainerModel(Swagger swagger, RefProperty schema, SwaggerRefPropertyFactory.PropertyContainerType containerType) {
    Model model = swagger.getDefinitions().get(schema.getSimpleRef());
    if (model == null || !(model instanceof ModelImpl)) {
      return null;
    }

    ModelImpl newSchemaModel = injectResultContainerModel(swagger, (ModelImpl) model, containerType);
    return newSchemaModel != null ? new RefProperty(newSchemaModel.getName(), RefFormat.INTERNAL) : null;
  }

  private ModelImpl injectResultContainerModel(Swagger swagger, ModelImpl innerModel, SwaggerRefPropertyFactory.PropertyContainerType containerType) {
    // Only inject result container model once.
    if (innerModel.getName().startsWith(resultContainerName)) {
      return null;
    }

    // Create result container model instance based on result container definition.
    Model model = swagger.getDefinitions().get(resultContainerName);
    if (model == null || !(model instanceof ModelImpl)) {
      return null;
    }
    ModelImpl resultContainerModel = (ModelImpl) model.clone();

    // Set unique name of result container model per container type to avoid overwriting documentation.
    resultContainerModel.setName(resultContainerName + "-" + innerModel.getName() + "-" + containerType);
    // Inject innerModel into 'data' property.
    Property dataProperty = SwaggerRefPropertyFactory.create(containerType, innerModel);
    dataProperty.setRequired(true);
    dataProperty.setDescription(createDataPropertyDescription(containerType));
    resultContainerModel.addProperty(resultContainerDataProperty, dataProperty);
    // Put result container model with injected innerModel into Swagger.
    swagger.addDefinition(resultContainerModel.getName(), resultContainerModel);

    return resultContainerModel;
  }

  private String createDataPropertyDescription(SwaggerRefPropertyFactory.PropertyContainerType containerType) {
    switch (containerType) {
      case NONE:
        return "Contains a single result";
      case LIST:
        return "Contains an array of results";
      default:
        return "";
    }
  }

}
