package poc;

import io.confluent.ksql.test.tools.KsqlTestingTool;

public class Runner {

  public static void main(String[] args) {
    String[] a = new String[] {
      "--input-file",
      "input.json",
      "--output-file",
      "output.json",
      "--sql-file",
      "ddl.sql",
      "--extension-dir",
      "extensions",
    };
    KsqlTestingTool.main(a);
  }
}
