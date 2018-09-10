package com.indigoid.utils;

import java.io.PrintStream;
import java.time.LocalDateTime;

/**
 * This is an utility class used to log error and info messages. The behavior of
 * this class depends on an system property that sets the level of "verbosity"
 * used.
 * 
 * @author Charlie
 */
public class MessageLogger {
	
	// TODO Implement this class to use log4j
	
	// TODO Allow logging to a file rather than logging to console

	/**
	 * Message types enumeration. Each error type has a severity. The higher the
	 * severity, the bigger the problem is. Also and error has an error type string
	 * used to print messages out.
	 */
	public enum MessageType {

		INFO("Info", 0), WARNING("Warning", 1), USAGE("Usage", 2), ERROR("ERROR", 2), SEVERE("SEVERE",
				3), FATAL("FATAL", 4);

		private String errorType;
		private int severity;

		private MessageType(String errorType, int severity) {
			this.errorType = errorType;
			this.severity = severity;
		}

		/**
		 * @return The error type as a string.
		 */
		public String getErrorType() {
			return this.errorType;
		}

		/**
		 * @return The severity associated to this error.
		 */
		public int getErrorSeverity() {
			return this.severity;
		}
	}

	/**
	 * Error manager verbose modes. Specifies the amount of error messages that are
	 * printed out.
	 */
	private enum VerboseMode {
		SILENT, // No messages at all
		QUIET, // [DEFAULT] Only messages directly managed by the program
		VERBOSE; //

		/**
		 * Converts a string into a VerboseMode value.
		 * 
		 * @param s
		 *            String representing the desired verbosity level. It is not case
		 *            sensitive.
		 * @return The VerboseMode for the string s or the default VerboseMode if s does
		 *         not match any of the existing modes.
		 */
		protected static VerboseMode fromString(String s) {
			if (s != null) {
				switch (s.toUpperCase()) {
				case "SILENT":
					return SILENT;
				case "QUIET":
					return QUIET;
				case "VERBOSE":
					return VERBOSE;
				}
			}
			return QUIET; // Default
		}
	}

	/**
	 * System property used to set the verbose mode for the error manager.
	 */
	private static final String VERBOSE_MODE_PROPERTY = "MessageLogger.mode";

	/**
	 * Errors equal or above this severity are written to stderror, otherwise to
	 * stdout
	 */
	private static final int BASE_ERR_SEVERITY = 2;

	/**
	 * Singleton design pattern. Unique instance
	 */
	private static final MessageLogger instance = new MessageLogger();

	/**
	 * Default program name (if cannot be retrieved from the stack trace).
	 */
	private static final String DEFAULT_PGM_NAME = "ProgramName";

	/**
	 * Actual program name (from the class called from the command line)
	 */
	private String programName;

	/**
	 * "Verbosity" mode. Default value is QUIET.
	 */
	private VerboseMode mode = VerboseMode.QUIET;

	/**
	 * This is where the non-error messages are logged.
	 */
	private PrintStream messageLog;

	/**
	 * This is where the error messages are logged.
	 */
	private PrintStream errorLog;

	/**
	 * Default constructor. Singleton design pattern.
	 */
	private MessageLogger() {

		// Retrieve program name (from the initial main class)
		programName = DEFAULT_PGM_NAME;
		StackTraceElement trace[] = Thread.currentThread().getStackTrace();
		if (trace.length > 0) {
			String firstClassName = trace[trace.length - 1].getClassName();
			int index = firstClassName.lastIndexOf(".");
			if (index >= 0) {
				programName = firstClassName.substring(index + 1);
			} else {
				programName = firstClassName;
			}
		}

		// Set verbose mode from System parameters
		mode = VerboseMode.fromString(System.getProperty(VERBOSE_MODE_PROPERTY));

		// Set the default logs to the console (stdout / stderr)
		messageLog = System.out;
		errorLog = System.err;
	}

	/**
	 * @return The unique instance of this class (singleton design pattern).
	 */
	public static MessageLogger getInstance() {
		return MessageLogger.instance;
	}

	/**
	 * @return The name of this program. This is the simple name of the class with
	 *         the main() method used to start current thread.
	 */
	public String getProgramName() {
		return this.programName;
	}

	/**
	 * Logs a message into the log mechanism used by this program. If verbose mode
	 * is SILENT, then nothing is logged. Otherwise, the message is logged to the
	 * message log (if severity < BASE_ERR_SEVERITY) or to the error log (if
	 * severity >= BASE_ERR_SEVERITY). Messages with severity zero are only logged
	 * on VERBOSE mode.
	 * 
	 * @param type
	 *            Type of message
	 * @param message
	 *            Message text
	 */
	public static void logMessage(MessageType type, String message) {
		if (instance.mode != VerboseMode.SILENT) {
			if (instance.mode == VerboseMode.VERBOSE || type.severity > 0) {
				writeToLog(
						LocalDateTime.now()/* .format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME) */ + ": "
								+ instance.programName + ": " + type.errorType + ": " + message,
						type.severity >= BASE_ERR_SEVERITY);
			}
		}
	}

	/**
	 * Writes a message to the application log.
	 * 
	 * @param message
	 *            Message text to write to the log
	 * @param toErrorLog
	 *            Use the error log flag (as opposite to the message log)
	 */
	private static void writeToLog(String message, boolean toErrorLog) {
		if (toErrorLog) {
			instance.errorLog.println(message);
		} else {
			instance.messageLog.println(message);
		}
	}

	/**
	 * Logs a message related to the usage (command line invocation) of the program
	 * with respect to the MessageLogger. It displays system variables that are used
	 * by this utility class.
	 */
	public static void logUsageMessage() {
		logMessage(MessageType.USAGE, "System variables that can be set:");
		logMessage(MessageType.USAGE, VERBOSE_MODE_PROPERTY + " = SILENT|QUIET|VERBOSE");
	}

	/**
	 * Logs information after an unexpected error condition. The amount of
	 * information logged depends on the verbosity mode. If mode is SILENT, then
	 * nothing is logged. Otherwise the message associated to the exception is
	 * logged. If mode is VERBOSE then the full stack trace of the exception is also
	 * logged.
	 * 
	 * @param e
	 *            The unmanaged exception.
	 */
	public static void logUnmanagedException(Exception e) {
		if (instance.mode != VerboseMode.SILENT) {
			logMessage(MessageType.FATAL, "Unexpected error condition at program " + instance.programName);
			logMessage(MessageType.FATAL, e.getMessage());
			if (instance.mode == VerboseMode.VERBOSE) {
				e.printStackTrace();
			}
		}
	}
}