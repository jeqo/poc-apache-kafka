package poc.stateful;

import org.apache.kafka.streams.StreamsBuilder;

public class StatefulTable {

  public static void main(String[] args) {
    var b = new StreamsBuilder();
    b.table("input").filter((key, value) -> true);
    System.out.println(b.build().describe());

    var b1 = new StreamsBuilder();
    b1.stream("input").filter((key, value) -> true).toTable();
    System.out.println(b1.build().describe());
  }
}
