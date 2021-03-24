package com.venkyraghav.confluent.kafka.util;

import org.apache.commons.cli.*;

import java.util.Optional;
import java.util.OptionalInt;

public class ParseCommandLine {

    public Boolean getNoValueArg(String application, CommandLine cmd, Options options, String shortArg, String longArg) {
        if (cmd.hasOption(longArg) || cmd.hasOption(shortArg)) {
            return true;
        }
        return false;
    }

    public Optional<Boolean> getBooleanArg(String application, CommandLine cmd, Options options, String shortArg, String longArg, Boolean defaultValue) {
        if (cmd.hasOption(longArg)) {
            return Optional.of(Boolean.parseBoolean(cmd.getOptionValue(longArg)));
        } else if (cmd.hasOption(shortArg)) {
            return Optional.of(Boolean.parseBoolean(cmd.getOptionValue(shortArg)));
        }
        return Optional.of(defaultValue);
    }

    public Optional<String> getStringArg(String application, CommandLine cmd, Options options, String shortArg, String longArg, String defaultValue) {
        if (cmd.hasOption(longArg)) {
            return Optional.of(cmd.getOptionValue(longArg));
        } else if (cmd.hasOption(shortArg)) {
            return Optional.of(cmd.getOptionValue(shortArg));
        }
        return Optional.of(defaultValue);
    }

    public OptionalInt getIntArg(String application, CommandLine cmd, Options options, String shortArg, String longArg, int defaultValue) {
        String value = "";
        try {
            if (cmd.hasOption(longArg)) {
                value = cmd.getOptionValue(longArg);
                return OptionalInt.of(Integer.parseInt(value));
            } else if (cmd.hasOption(shortArg)) {
                value = cmd.getOptionValue(shortArg);
                return OptionalInt.of(Integer.parseInt(value));
            }
        } catch (NumberFormatException e) {
            showHelp(application, options, shortArg, longArg, value, e.getMessage());
        }
        return OptionalInt.of(defaultValue);
    }

    public void showHelp(String application, Options options, String shortArg, String longArg, String value, String message) {
        HelpFormatter formatter = new HelpFormatter();
        System.out.println("Show Usage: " + message);
        formatter.printHelp(application, options);
        System.exit(1);
    }
}
