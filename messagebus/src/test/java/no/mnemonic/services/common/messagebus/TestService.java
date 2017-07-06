package no.mnemonic.services.common.messagebus;

import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.Service;

public interface TestService extends Service {

  String getString(String arg);

  String primitiveLongArgument(long arg);

  String primitiveIntArgument(int arg);

  String primitiveCharArgument(char arg);

  String primitiveByteArgument(byte arg);

  String primitiveFloatArgument(float arg);

  String primitiveDoubleArgument(double arg);

  String primitiveBooleanArgument(boolean arg);

  String objectArrayArgument(String[] arg);

  String primitiveArrayArgument(long[] arg);

  ResultSet<String> getResultSet(String arg);
}
