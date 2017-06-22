package no.mnemonic.services.common.messagebus;

import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.Service;

public interface TestService extends Service {

  String getString(String arg);

  ResultSet<String> getResultSet(String arg);
}
