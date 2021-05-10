package interceptor;

import org.apache.kafka.common.header.Headers;

public class CloudEventHeaders {

  public boolean validate(Headers headers) {
    if (headers.lastHeader("ce-id") == null) {
      return false;
    } else {
      return true;
    }
  }
}
