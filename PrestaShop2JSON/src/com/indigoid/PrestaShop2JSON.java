package com.indigoid;

import com.indigoid.utils.MessageLogger;
import com.indigoid.utils.MessageLogger.MessageType;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import com.indigoid.dbutils.MariaDBConnectionManager;
import com.indigoid.prestashop.CustomersLoader;
import com.indigoid.prestashop.OrdersLoader;
import com.indigoid.prestashop.ProductsLoader;

// TODO Implement the application as a JSP page

/**
 * Class with the main() method. This application is build to read a PrestaShop
 * database and to export its contents in the form of JSON objects. This can
 * used as an intermediate step in order to migrate the data to a database
 * supporting this object model, as for instance MongoDB.
 * 
 * @author Charlie
 */
public class PrestaShop2JSON {

	/**
	 * Type of entity that the program will extract from the DB.
	 */
	private enum EntityToExtract {
		CUSTOMERS, PRODUCTS, ORDERS, ALL, NOT_SET
	}

	//
	// Constants
	//
	/**
	 * Default database user name.
	 */
	private static final String DEFAULT_USER = "root";
	/**
	 * Default password for default user.
	 */
	private static final String DEFAULT_PASSWD = "";
	/**
	 * Command line arguments processed by this program
	 */
	private static final String COMMAD_LINE_ARGUMENTS = "<entity> <host> <port> <database> [<user> [<password>]]";
	/**
	 * Usage instructions for parameter entity
	 */
	private static final String ENTITY_USAGE_MESSAGE = "entity: Customers|Products|Orders|All ";
	//
	// Error message strings
	//
	private static final String UNUSED_PARAMETER_IS_IGNORED = "Unused parameter is ignored: ";
	private static final String INCORRECT_PORT_NUMBER = "Incorrect port number: ";
	private static final String INCORRECT_NUMBER_OF_PARAMETERS_AT_PROGRAM_INVOCATION = "Incorrect number of parameters at program invocation.";
	private static final String INVALID_VALUE_FOR_PARAMETER_ENTITY = "Invalid value for parameter <entity>: ";
	//
	// Properties
	//
	private static final String PROPERTIES_FILE_EXTENSION = ".properties";
	private static final String ORDERS_JSON_PROPERTY = "orders_json";
	private static final String PRODUCTS_JSON_PROPERTY = "products_json";
	private static final String CUSTOMERS_JSON_PROPERTY = "customers_json";
	private static final String SHOP_NAME_PROPERTY = "shop_name";
	//
	// Properties defaults
	//
	private static final String DEFAULT_CUSTOMERS_JSON_FILE_NAME = "./customers.json";
	private static final String DEFAULT_PRODUCTS_JSON_FILE_NAME = "./products.json";
	private static final String DEFAULT_ORDERS_JSON_FILE_NAME = "./orders.json";
	private static final String DEFAULT_SHOP_NAME = "Prestashop";

	private static final int EXIT_CODE_NORMAL = 0;
	private static final int EXIT_CODE_ERROR = -1;
	private static final int MIN_CMD_LINE_ARGS = 4;
	private static final int MAX_CMD_LINE_ARGS = 6;
	private static final int INVALID_PORT = -1;

	/**
	 * Properties for the program. They are set via a external .properties file
	 */
	private static Properties prop = new Properties();

	/**
	 * Main method for this program. Just parses command line arguments and calls to
	 * the MainProcess() which is the real responsible of the processing.
	 * 
	 * @param args
	 *            External arguments passed from the command line (some are
	 *            required). Usage: host port database [user [password]]
	 */
	public static void main(String[] args) {

		int argc = 0; // Counter, argument being consumed

		// Arguments check
		if (args.length < MIN_CMD_LINE_ARGS) {
			MessageLogger.logMessage(MessageType.ERROR, INCORRECT_NUMBER_OF_PARAMETERS_AT_PROGRAM_INVOCATION);
			PrestaShop2JSON.printUsage();
			System.exit(EXIT_CODE_ERROR); // Exit program
		}

		// Argument 1: Entity to extract
		String entityStr = args[argc++];
		EntityToExtract entity = EntityToExtract.NOT_SET;
		switch (entityStr.toUpperCase()) {
		case "CUSTOMERS":
			entity = EntityToExtract.CUSTOMERS;
			break;
		case "PRODUCTS":
			entity = EntityToExtract.PRODUCTS;
			break;
		case "ORDERS":
			entity = EntityToExtract.ORDERS;
			break;
		case "ALL":
			entity = EntityToExtract.ALL;
			break;
		default:
			MessageLogger.logMessage(MessageType.ERROR, INVALID_VALUE_FOR_PARAMETER_ENTITY + entityStr);
			PrestaShop2JSON.printUsage();
			System.exit(EXIT_CODE_ERROR); // Exit program
		}

		// Argument 2: Database hostname
		String host = args[argc++];

		// Argument 3: Database port - MUST be a number (integer)
		String portString = args[argc++];
		int port = INVALID_PORT;
		try {
			port = Integer.parseInt(portString);
		} catch (NumberFormatException e) {
			// Exception is ignored but a negative port number will cause the program to
			// fail
		}

		// Port must be a positive number
		if (port < 0) {
			MessageLogger.logMessage(MessageType.ERROR, INCORRECT_PORT_NUMBER + portString);
			PrestaShop2JSON.printUsage();
			System.exit(EXIT_CODE_ERROR); // Exit program
		}

		// Argument 4: Database name
		String database = args[argc++];

		// Optional parameters set to defaults
		String dbUser = PrestaShop2JSON.DEFAULT_USER;
		String dbPasswd = PrestaShop2JSON.DEFAULT_PASSWD;

		// Process optional parameters, if any
		if (args.length > MIN_CMD_LINE_ARGS) {

			// Argument 5: user for the database
			dbUser = args[argc++];

			if (args.length > (MIN_CMD_LINE_ARGS + 1)) {

				// Argument 6: password for the database user
				dbPasswd = args[argc++];
			}
		}

		// Ignore the rest of the arguments. Will not crash the program
		if (args.length > MAX_CMD_LINE_ARGS) {
			PrestaShop2JSON.printUsage();
			while (argc < args.length) {
				MessageLogger.logMessage(MessageType.WARNING,
						UNUSED_PARAMETER_IS_IGNORED + args[argc++] + " [" + (argc) + "]");
			}
		}

		// Load properties from the external .properties file
		loadProperties();

		// Done with parameters parsing. Call the main process for this program.
		int exitCode = MainProcess(entity, host, port, database, dbUser, dbPasswd);

		// Terminate with the exit code from the main process.
		System.exit(exitCode);
	}

	/**
	 * Performs the main process for this program (convert a PrestaShop DB into
	 * JSON).
	 * 
	 * @param entity
	 *            Type of entity to download
	 * @param host
	 *            Database server host name (localhost)
	 * @param port
	 *            Database server port number (3306)
	 * @param database
	 *            Database including PrestaShop tables
	 * @param dbUser
	 *            User to connect to the database server (must have read access to
	 *            PrestaShop database)
	 * @param dbPasswd
	 *            Password of the database user
	 * @return Zero (EXIT_CODE_NORMAL) if the process has been completed
	 *         successfully. A negative number (EXIT_CODE_ERROR) otherwise.
	 */
	public static int MainProcess(EntityToExtract entity, String host, int port, String database, String dbUser,
			String dbPasswd) {

		int exitCode = EXIT_CODE_NORMAL;

		MessageLogger.logMessage(MessageType.INFO, "About to start database processing");

		/*
		 * This is the outer error control block. Any unhandled exception happening
		 * inside the program will be caught here. Main purpose of this is to close
		 * connections and free resources.
		 */
		// Holds database connection and other resources opened against the DB manager
		try (MariaDBConnectionManager dbc = new MariaDBConnectionManager(host, port, database, dbUser, dbPasswd)) {

			//
			// Extract customers data
			//
			if (entity == EntityToExtract.CUSTOMERS || entity == EntityToExtract.ALL) {
				MessageLogger.logMessage(MessageType.INFO, "About to load Customers data");
				CustomersLoader cl = new CustomersLoader();

				int nCustomers = cl.load(dbc, prop.getProperty(SHOP_NAME_PROPERTY));

				MessageLogger.logMessage(MessageType.INFO, nCustomers + " customers has been loaded");

				// Write customers data to output file
				cl.dumpData(prop.getProperty(CUSTOMERS_JSON_PROPERTY));

				MessageLogger.logMessage(MessageType.INFO,
						nCustomers + " customer documents written to " + prop.getProperty(CUSTOMERS_JSON_PROPERTY));
			}

			//
			// Extract products data
			//
			if (entity == EntityToExtract.PRODUCTS || entity == EntityToExtract.ALL) {
				MessageLogger.logMessage(MessageType.INFO, "About to load Products data");
				ProductsLoader pl = new ProductsLoader();

				int nProducts = pl.load(dbc, prop.getProperty(SHOP_NAME_PROPERTY));

				MessageLogger.logMessage(MessageType.INFO, nProducts + " products has been loaded");

				// Write customers data to output file
				pl.dumpData(prop.getProperty(PRODUCTS_JSON_PROPERTY));

				MessageLogger.logMessage(MessageType.INFO,
						nProducts + " product documents written to " + prop.getProperty(PRODUCTS_JSON_PROPERTY));
			}

			//
			// Extract orders data
			//
			if (entity == EntityToExtract.ORDERS || entity == EntityToExtract.ALL) {
				MessageLogger.logMessage(MessageType.INFO, "About to load Orders data");
				OrdersLoader ol = new OrdersLoader();

				int nOrders = ol.load(dbc, prop.getProperty(SHOP_NAME_PROPERTY));

				MessageLogger.logMessage(MessageType.INFO, nOrders + " orders has been loaded");

				// Write orders data to output file
				ol.dumpData(prop.getProperty(ORDERS_JSON_PROPERTY));

				MessageLogger.logMessage(MessageType.INFO,
						nOrders + " order documents written to " + prop.getProperty(ORDERS_JSON_PROPERTY));
			}

		} catch (Exception e) {
			MessageLogger.logUnmanagedException(e);
			exitCode = EXIT_CODE_ERROR;
		}

		MessageLogger.logMessage(MessageType.INFO, "Process completed. Exit code: " + exitCode);
		return exitCode;
	}

	/**
	 * Load the properties file for this program and if not present, initializes the
	 * properties using default values.
	 */
	private static void loadProperties() {

		// Load properties file
		try (FileInputStream fis = new FileInputStream(
				MessageLogger.getInstance().getProgramName() + PROPERTIES_FILE_EXTENSION)) {
			prop.load(fis);
		} catch (FileNotFoundException e1) {
			// If not found, properties will be set to its default values
		} catch (IOException e2) {
			MessageLogger.logUnmanagedException(e2);
		}

		// If not set, set properties to its default values
		if (!prop.containsKey(CUSTOMERS_JSON_PROPERTY)) {
			prop.setProperty(CUSTOMERS_JSON_PROPERTY, DEFAULT_CUSTOMERS_JSON_FILE_NAME);
		}
		if (!prop.containsKey(PRODUCTS_JSON_PROPERTY)) {
			prop.setProperty(PRODUCTS_JSON_PROPERTY, DEFAULT_PRODUCTS_JSON_FILE_NAME);
		}
		if (!prop.containsKey(ORDERS_JSON_PROPERTY)) {
			prop.setProperty(ORDERS_JSON_PROPERTY, DEFAULT_ORDERS_JSON_FILE_NAME);
		}
		if (!prop.containsKey(SHOP_NAME_PROPERTY)) {
			prop.setProperty(SHOP_NAME_PROPERTY, DEFAULT_SHOP_NAME);
		}
	}

	/**
	 * Print the "Usage" error message for this program.
	 */
	private static void printUsage() {
		// This program command-line invocation
		MessageLogger.logMessage(MessageType.USAGE,
				MessageLogger.getInstance().getProgramName() + " " + COMMAD_LINE_ARGUMENTS);
		MessageLogger.logMessage(MessageType.USAGE,
				MessageLogger.getInstance().getProgramName() + " " + ENTITY_USAGE_MESSAGE);

		// Other usage directions used by the ErrorManager
		MessageLogger.logUsageMessage();
	}
}