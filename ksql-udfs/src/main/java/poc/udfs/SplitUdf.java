package poc.udfs;

import io.confluent.common.Configurable;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@UdfDescription(
    name = "custom_split",
    author = "jeqo",
    version = "0.1.0",
    description = "custom split a string into an array")
public class SplitUdf implements Configurable {

  @Override
  public void configure(Map<String, ?> map) {
  }

  @Udf(description = "split")
  public List<String> split(@UdfParameter(value = "v") String v) {
    return Arrays.asList(v.split(" "));
  }
}
