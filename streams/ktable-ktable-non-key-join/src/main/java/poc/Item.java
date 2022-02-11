package poc;

import java.util.Map;

public record Item(String id, String parent, String name, Map<String, String> attributes) {
  Item addAttrs(Map<String, String> attributes) {
    if (attributes != null) {
      this.attributes.putAll(attributes);
    }
    return this;
  }
}
