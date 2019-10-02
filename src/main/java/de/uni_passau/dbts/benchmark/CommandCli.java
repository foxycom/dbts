package de.uni_passau.dbts.benchmark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.uni_passau.dbts.benchmark.conf.Constants;

/**
 * CLI arguments parser. Currently, only the option -cf is supported.
 */
public class CommandCli {
  private final String HELP_ARGS = "help";
  private final String CONFIG_ARGS = "cf";
  private final String CONFIG_NAME = "config file";

  private static final int MAX_HELP_CONSOLE_WIDTH = 88;

  /**
   * Registers CLI options.
   *
   * @return CLI options.
   */
  private Options createOptions() {
    Options options = new Options();
    Option help = new Option(HELP_ARGS, false, "Display help information");
    help.setRequired(false);
    options.addOption(help);

    Option config =
        Option.builder(CONFIG_ARGS)
            .argName(CONFIG_NAME)
            .hasArg()
            .desc("Config file path")
            .build();
    options.addOption(config);

    return options;
  }

  /**
   * Parses the CLI input.
   *
   * @param parser Parser.
   * @param options CLI options.
   * @param args CLI input.
   * @param hf Formatter.
   * @return true if CLI option were entered correctly, false otherwise.
   */
  private boolean parseParams(
      CommandLineParser parser,
      Options options,
      String[] args,
      HelpFormatter hf) {
    try {
      CommandLine commandLine = parser.parse(options, args);
      if (commandLine.hasOption(HELP_ARGS)) {
        hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
        return false;
      }

      if (commandLine.hasOption(CONFIG_ARGS)) {
        System.setProperty(Constants.BENCHMARK_CONF, commandLine.getOptionValue(CONFIG_ARGS));
      }

    } catch (ParseException e) {
      System.out.println("Require more params input, please check the following hint.");
      hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
      return false;
    } catch (Exception e) {
      System.out.println("Wrong params input, because " + e.getMessage());
      hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
      return false;
    }
    return true;
  }

  /**
   * Initializes the parser.
   *
   * @param args CLI input.
   * @return true if options were entered correctly, false otherwise.
   */
  public boolean init(String[] args) {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      System.out.println("Require more params input, please check the following hint.");
      hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
      return false;
    }
    return parseParams(parser, options, args, hf);
  }

  public static void main(String[] args) {}
}
