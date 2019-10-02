package de.uni_passau.dbts.benchmark.sersyslog;

/**
 * System monitoring utility.
 */
public class UsageUtils {

  /**
   * Parses the shell output of tools that read the system metrics.
   *
   * @param input Shell line.
   * @return A parsed line.
   */
  public static String parseShellValues(String input) {
    input = input.trim().replaceAll(",", ".");
    if (input.startsWith("\t")) {
      input = input.substring(1).trim();
    }
    return input;
  }
}
