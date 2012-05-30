package com.landonia.vertica;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import org.apache.log4j.Logger;

import com.vertica.Driver;
import com.vertica.PGConnection;

/**
 * Simple JDBC wrapper class to test queries against the Vertica Driver.
 * 
 * @author Landon Wainwright
 */
public class VerticaQueryExecuter {

	private static final String verticaDriverString = "com.vertica.Driver";
	private static final String inputRequestString = "\nEnter query (type 'exit' to finish program):\n";
	private static final String inputRowCountString = "\nEnter number of rows to fetch (type '-1' to finish):\n";
	private static final String inputRowSkipCountString = "\nEnter number of rows to skip:\n";
	private static Logger log = Logger.getLogger(VerticaQueryExecuter.class);
	private String connectionURL;
	private Connection connection = null;
	private String userName;
	private String password;

	public VerticaQueryExecuter(String connectionURL) {
		// Set the connection string
		setConnectionURL(connectionURL);
	}

	/**
	 * Makes the connection to the provided connection url.
	 * 
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public void connect() throws ClassNotFoundException, SQLException {
		log.debug("Connecting to datasource...");

		// Load the Driver class.
		Class.forName(verticaDriverString);

		// Create the connection using the static getConnection method
		setConnection(DriverManager.getConnection(connectionURL, getUserName(),
				getPassword()));

		// Update the connection to a lower buffer to start getting results
		// immediately
		if (getConnection() instanceof PGConnection) {
			// Set the buffer size to smallest possible
			((PGConnection) getConnection()).setMaxLRSMemory(256);
		}
	}

	/**
	 * Disconnects the current connection.
	 * 
	 * @throws SQLException
	 */
	public void disconnect() throws SQLException {
		log.debug("Disconnecting from datasource...");
		if (getConnection() != null && !getConnection().isClosed())
			getConnection().close();
	}

	/**
	 * Reads statements from the provided input stream and outputs the results
	 * to the output stream.
	 * 
	 * @param in
	 *            the input stream where to read user queries from
	 * @param out
	 *            the output stream to write the response to
	 */
	public void executeStatements(Scanner in, PrintStream out) {
		// Read the queries from the command line
		out.println(inputRequestString);
		String query = "";
		while (query.equals(""))
			query = in.nextLine();
		while (query != null && !query.equals("exit")) {
			Long startTime = System.currentTimeMillis();
			ResultSet result;
			try {
				result = executeStatement(query);
				log.debug("Total execution time (ms): "
						+ (System.currentTimeMillis() - startTime));
				parseResultSet(result, in, out);
			} catch (SQLException e) {
				log.error("SQLException", e);
			}
			out.println(inputRequestString);
			out.println();
			query = "";
			while (query.equals(""))
				query = in.nextLine();
		}
	}

	/**
	 * Executes the provided query statement againt the current connection
	 * 
	 * @param statement
	 *            the query to execute against the datasource
	 * @return
	 * @throws SQLException
	 */
	public ResultSet executeStatement(String statement) throws SQLException {
		// Ensure there is a connection
		if (getConnection() == null || getConnection().isClosed()) {
			log.debug("Cannot execute statement as the connection has been closed");
			return null;
		}
		log.debug("Executing statement: '" + statement + "'");
		// Create a Statement class to execute the SQL statement
		Statement stmt = getConnection().createStatement();

		// Execute the SQL statement and get the results in a Resultset
		return stmt.executeQuery(statement);
	}

	/**
	 * Skips the position relative to the current position.
	 * 
	 * @param result
	 *            the result from the statement execution
	 * @param in
	 *            the input stream where to read user queries from
	 * @param out
	 *            the output stream to write the response to
	 * @throws SQLException
	 */
	private void skipPosition(ResultSet result, Scanner in, PrintStream out)
			throws SQLException {
		if (result.getType() == ResultSet.TYPE_FORWARD_ONLY)
			out.println("Result set supports forward only cursor");
		else {
			out.println(inputRowSkipCountString);
			out.println();
			int skipPos = in.nextInt();
			if (result.relative(skipPos)) {
				out.println("Cursor moved " + skipPos + " rows");
			} else {
				out.println("Cursor not moved");
			}
		}
		out.println();
	}

	/**
	 * Parses the given <code>ResultSet</code> by outputting the total rows to
	 * the output stream.
	 * 
	 * @param result
	 *            the result from the statement execution
	 * @param in
	 *            the input stream where to read user queries from
	 * @param out
	 *            the output stream to write the response to
	 * @throws SQLException
	 */
	public void parseResultSet(ResultSet result, Scanner in, PrintStream out)
			throws SQLException {
		log.debug("Retrieved result set.");
		out.println("Result set retrieved, extracting meta information...");
		ResultSetMetaData meta = result.getMetaData();
		// Ask to skip relative to the current position
		skipPosition(result, in, out);
		// Read the number of rows that the user wants to retrieve.
		out.println(inputRowCountString);
		out.println();
		int noRows = in.nextInt();
		while (noRows != -1) {
			Long startTime = System.currentTimeMillis();
			// print out the headings
			printColumns(meta, in, out);
			for (int i = 0; i < noRows; i++) {
				// Move to the next row
				result.next();
				// Print the row
				printRow(result, meta, in, out);
				if (result.isLast()) {
					out.println("\nNo more rows..");
					break;
				}
			}
			out.println("\n");
			log.debug("Total return time (ms): "
					+ (System.currentTimeMillis() - startTime));

			if (!result.isLast()) {
				// Ask to skip relative to the current position
				skipPosition(result, in, out);
				// Ask for how many rows to return
				System.out.println(inputRowCountString);
				noRows = in.nextInt();
			} else {
				noRows = -1;
			}
		}
		// close this connection as we are now finished with it
		result.close();
		out.println("\n");
	}

	/**
	 * Prints out the headers for the result meta information provided
	 * 
	 * @param meta
	 *            the meta information from ta result set
	 * @param in
	 *            the input stream where to read user queries from
	 * @param out
	 *            the output stream to write the response to
	 * @throws SQLException
	 */
	private void printColumns(ResultSetMetaData meta, Scanner in,
			PrintStream out) throws SQLException {
		int numberOfColumns = meta.getColumnCount();
		out.println();
		out.print("|-");
		for (int i = 1; i <= numberOfColumns; i++) {
			out.print(meta.getColumnLabel(i));
			for (int j = 0; j < meta.getColumnDisplaySize(i)
					- meta.getColumnLabel(i).length(); j++) {
				out.print("-");
			}
			out.print("|");
		}
		out.println();
	}

	/**
	 * 
	 * @param result
	 *            the result set containing the row data
	 * @param meta
	 *            the meta information from ta result set
	 * @param in
	 *            the input stream where to read user queries from
	 * @param out
	 *            the output stream to write the response to
	 * @throws SQLException
	 */
	private void printRow(ResultSet result, ResultSetMetaData meta, Scanner in,
			PrintStream out) throws SQLException {
		// Print out the row detail
		int valueLength;
		int numberOfColumns = meta.getColumnCount();
		out.println();
		out.print("| ");
		for (int i = 1; i <= numberOfColumns; i++) {
			String value = result.getString(i);
			out.print(value);
			valueLength = value != null ? value.length() : 4;
			for (int j = 0; j < meta.getColumnDisplaySize(i) - valueLength; j++) {
				out.print(" ");
			}
			out.print("|");
		}
	}

	public void setConnectionURL(String connectionURL) {
		log.debug("Setting connection URL: " + connectionURL);
		this.connectionURL = connectionURL;
	}

	public String getConnectionURL() {
		return connectionURL;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getUserName() {
		return userName;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getPassword() {
		return password;
	}

	/**
	 * Run the vertica connector to query a vertica db instance.
	 * 
	 * args[0]: the vertica connection URL
	 * args[1]: the username for this connection session
	 * args[2]: the password for this connection session
	 * 
	 * @param args the arguments required for this program to execute
	 */
	public static void main(String args[]) {
		// Extract the connection string and start the test
		if (args.length > 0) {
			log.debug("Starting new Vertica CLI Connector");
			VerticaQueryExecuter ve = new VerticaQueryExecuter(args[0]);
			if (args.length > 1) {
				// Set the username
				ve.setUserName(args[1]);
			}
			if (args.length > 2) {
				// Set the password
				ve.setPassword(args[2]);
			}
			try {
				// Connect and make the query
				ve.connect();
				ve.executeStatements(new Scanner(System.in), System.out);
				ve.disconnect();
				log.debug("Disconnected.");
			} catch (ClassNotFoundException e) {
				log.error("ClassNotFoundException", e);
			} catch (SQLException e) {
				log.error("SQLException", e);
			}
		} else {
			// Cant execute the query
			log.error("Cannot run program - missing connection URL");
			throw new RuntimeException(
					"You must include the JDBC connection string");
		}
	}
}