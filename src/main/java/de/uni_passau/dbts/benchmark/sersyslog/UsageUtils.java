package de.uni_passau.dbts.benchmark.sersyslog;

public class UsageUtils {
  public static String parseShellValues(String input) {
    input = input.trim().replaceAll(",", ".");
    if (input.startsWith("\t")) {
      input = input.substring(1).trim();
    }
    return input;
  }
}
