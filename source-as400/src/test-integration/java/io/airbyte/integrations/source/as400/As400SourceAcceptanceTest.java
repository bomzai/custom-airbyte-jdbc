/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.as400;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.cdk.integrations.standardtest.source.SourceAcceptanceTest;
import io.airbyte.cdk.integrations.standardtest.source.TestDestinationEnv;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import java.util.HashMap;
import org.junit.jupiter.api.Disabled;

@Disabled
public class As400SourceAcceptanceTest extends SourceAcceptanceTest {

  private JsonNode config;

  @Override
  protected void setupEnvironment(final TestDestinationEnv testEnv) {
    // TODO create new container. Ex: "new OracleContainer("epiclabs/docker-oracle-xe-11g");"
    // TODO make container started. Ex: "container.start();"
    // TODO init JsonNode config
    // TODO crete airbyte Database object "Databases.createJdbcDatabase(...)"
    // TODO insert test data to DB. Ex: "database.execute(connection-> ...)"
    // TODO close Database. Ex: "database.close();"
  }

  @Override
  protected void tearDown(final TestDestinationEnv testEnv) {
    // TODO close container that was initialized in setup() method. Ex: "container.close();"
  }

  @Override
  protected String getImageName() {
    return "airbyte/source-as400:dev";
  }

  @Override
  protected ConnectorSpecification getSpec() throws Exception {
    return Jsons.deserialize(MoreResources.readResource("spec.json"), ConnectorSpecification.class);
  }

  @Override
  protected JsonNode getConfig() {
    return config;
  }

  @Override
  protected ConfiguredAirbyteCatalog getConfiguredCatalog() {
    // TODO Return the ConfiguredAirbyteCatalog with ConfiguredAirbyteStream objects
    return null;
  }

  @Override
  protected JsonNode getState() {
    return Jsons.jsonNode(new HashMap<>());
  }

}
