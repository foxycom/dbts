package de.uni_passau.dbts.benchmark.sersyslog;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/** IO metrics reader. */
public enum IoUsage {
  INSTANCE;

  /**
   * Reads io from iostat. Its sample output looks like:
   *
   * <p>Linux 5.0.0-21-generic (tim-ba-client) 08/27/2019 _x86_64_ (4 CPU)
   *
   * <p>Device r/s w/s rkB/s wkB/s rrqm/s wrqm/s %rrqm %wrqm r_await w_await aqu-sz rareq-sz
   * wareq-sz svctm %util loop0 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 17.07 0.00 0.00 1.54 0.00
   * 0.27 0.00 loop1 0.01 0.00 0.01 0.00 0.00 0.00 0.00 0.00 0.20 0.00 0.00 1.07 0.00 0.12 0.00
   * loop2 0.01 0.00 0.01 0.00 0.00 0.00 0.00 0.00 0.07 0.00 0.00 1.07 0.00 0.11 0.00 loop3 0.00
   * 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.03 0.00 0.00 5.09 0.00 0.42 0.00 loop4 0.00 0.00 0.00 0.00
   * 0.00 0.00 0.00 0.00 0.05 0.00 0.00 5.06 0.00 0.47 0.00 loop5 0.00 0.00 0.00 0.00 0.00 0.00 0.00
   * 0.00 1.25 0.00 0.00 1.00 0.00 1.00 0.00 sda 0.02 0.36 0.39 7.68 0.00 0.12 22.85 25.62 6.61
   * 18.09 0.01 23.96 21.34 0.99 0.04
   *
   * <p>Device r/s w/s rkB/s wkB/s rrqm/s wrqm/s %rrqm %wrqm r_await w_await aqu-sz rareq-sz
   * wareq-sz svctm %util loop0 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00
   * 0.00 0.00 loop1 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00
   * loop2 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 loop3 0.00
   * 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 loop4 0.00 0.00 0.00 0.00
   * 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 loop5 0.00 0.00 0.00 0.00 0.00 0.00 0.00
   * 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 sda 0.00 4028.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00
   * 0.40 0.00 0.00 0.00 0.25 100.00
   *
   * @param driveName Name of the disk drive to monitor.
   * @return Pair of reads and writes operations per second.
   */
  public Map<String, Float> get(String driveName) {
    float readsPerSec = 0.0f;
    float writesPerSec = 0.0f;
    Map<String, Float> values = new HashMap<>();
    Process process;
    Runtime r = Runtime.getRuntime();
    try {
      String command = "iostat -xd 1 2";
      process = r.exec(command);
      BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      boolean rightSection = false;
      while ((line = input.readLine()) != null) {
        if (line.contains(driveName)) {
          if (!rightSection) {
            rightSection = true;
          } else if (rightSection) {
            line = UsageUtils.parseShellValues(line);
            String[] temp = line.split("\\s+");
            readsPerSec = Float.parseFloat(temp[1]);
            writesPerSec = Float.parseFloat(temp[2]);
            break;
          }
        }
      }
    } catch (IOException e) {
      System.err.println("Could not read IO metrics because: " + e.getMessage());
    }
    values.put("readsPerSec", readsPerSec);
    values.put("writesPerSec", writesPerSec);
    return values;
  }
}
