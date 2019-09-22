package de.uni_passau.dbts.benchmark;

import de.uni_passau.dbts.benchmark.conf.Constants;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

public class Test {

  public static void main(String[] args) throws Exception {
    DateTime date = new DateTime(Constants.START_TIME);
    LocalDate startDate = date.toLocalDate();
    LocalDate endDate = startDate.plusDays(1);
    String debug = String.format(
        "CREATE TABLE test_2018_08_00 PARTITION OF test FOR VALUES FROM ('%s') TO ('%s');",
        startDate, endDate);
    System.out.println(debug);
  }
}
