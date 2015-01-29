package com.aexp.warehouse.exception;

public class InvalidPDRecordException extends Exception {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 8118773264717429793L;

}

----------------------------------------------------------------------
package com.aexp.warehouse.exception;

public class TableLockDetectedException extends Exception {

	public TableLockDetectedException() {
		System.out
				.println("TableLockDetectedException : Table is locked by the another process");
	}

	public TableLockDetectedException(String processID) {
		System.out
				.println("TableLockDetectedException : Table is locked by the process "
						+ processID);
	}

}

-----------------------------------------------------------------------------------------

package com.amex.warehouse.configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.log4j.Priority;

/**
 * 
 * The Class ETLConfiguration.
 */

/**
 * @author JAVA
 *
 */
public class ETLConfiguration {

	private int HbaseTableID;

	/** The H tbale. */

	private String HTbale;

	/** The Feed id. */

	private int FeedID;

	/** The Seperator. */

	private String Seperator;

	/** The Header column mapping. */

	private LinkedHashMap<String, String> HeaderColumnMapping;

	/** The headers. */

	private String headers;

	/** The columns. */

	private String columns;

	/** The is key array. */

	private String isKeyArray;

	/** The rowkey format. */

	private String rowkeyFormat;

	private String tableID;

	private String runFeedID;

	private String localConfigFilePath;

	private String outputDir;

	private String bulkLoadStatusDir;

	private String tableLockFileDir;

	private static Connection connect = null;

	/* Changes starts here */

	boolean isHeadPresent = false;

	boolean isTailPresent = false;

	public boolean isHeadPresent() {

		return isHeadPresent;
	}

	public void setHeadPresent(boolean isHeadPresent) {
		this.isHeadPresent = isHeadPresent;
	}

	public boolean isTailPresent() {

		return isTailPresent;

	}

	public void setTailPresent(boolean isTailPresent) {
		this.isTailPresent = isTailPresent;
	}

	private static Properties properties = new Properties();

	/* Changes ends here */

	/**
	 * 
	 * This will be Singleton class as the same instance will be shared with
	 * Driver program.
	 * 
	 * */

	private static ETLConfiguration config;

	/**
	 * 
	 * Instantiates a new ETL configuration.
	 */

	/* Changes starts here */

	/**
	 * Initializing Database Connection.
	 * 
	 * @throws SQLException
	 * 
	 * @throws ClassNotFoundException
	 */
	public static void initializeConnection()

	throws SQLException, ClassNotFoundException {

		String dbName = "pmcdb";//

		String driverClass = "com.mysql.jdbc.Driver";

		String connectionURL = "jdbc:mysql://10.74.132.38:3306/" + dbName;

		String userName = "pmcprod";

		String password = "pmcprod";

		Class.forName(driverClass);

		if (connect == null)

			connect = DriverManager.getConnection(connectionURL + "?" + "user="

			+ userName + "&password=" + password);

	}

	/* Changes starts here */

	/**
	 * This method Load a properties file from the file system and retrieves the
	 * property value.
	 * 
	 */
	private static boolean loadPropertyFileFromFileSystem() {

		boolean isPropertyFileLoaded = false;

		try {

			properties.load(new FileInputStream("control.properties"));

			isPropertyFileLoaded = true;

		} catch (Exception exception) {

			exception.printStackTrace();

		}

		return isPropertyFileLoaded;

	}

	/**
	 * This method initializes properties from property file
	 * 
	 */

	private boolean initializePropertiesFromPropertyFile() {

		boolean isPropertyFileInitialized = false;

		try {

			if (loadPropertyFileFromFileSystem()) {

				setOutputDir(properties.getProperty("OUTPUT_DIRECTORY_PATH"));

				// String a = properties.getProperty("OUTPUT_DIRECTORY_PATH");

				String headPresent = properties.getProperty("Head");

				if (headPresent.equalsIgnoreCase("true"))

				{
					setHeadPresent(true);
				}

				String tailPresent = properties.getProperty("Tail");

				if (tailPresent.equalsIgnoreCase("true"))

				{
					setTailPresent(true);
				}

				isPropertyFileInitialized = true;

			}

		} catch (Exception exception) {

			exception.printStackTrace();

		}

		return isPropertyFileInitialized;

	}

	/* Changes ends here */

	/**
	 * Retrieves OutputDirectory Name using Database Connection.
	 * 
	 */

	private String getOutPutDirectoryNameFromDatabase() {

		String outPutDirectoryName = null;

		try {

			initializeConnection();

			String query = "SELECT Outputpath FROM pmc_feed ";// TODO:Add
																// constraint
																// which o/p to
																// select

			Statement statement = connect.createStatement();

			ResultSet resultSet = statement.executeQuery(query);

			while (resultSet.next()) {

				outPutDirectoryName = resultSet.getString("Outputpath");

			}

			/* Changes starts here */

			resultSet.close();

			/* Changes ends here */

			statement.close();

		} catch (Exception exception) {
			System.out.println("Exception Generated...");
			exception.printStackTrace();

		}
		return outPutDirectoryName;

	}

	private ETLConfiguration() {

		super();

		/* Update starts here */

		initializePropertiesFromPropertyFile();

		// this.outputDir = getOutPutDirectoryNameFromDatabase();//Retrieving
		// From Database..

		/* Update ends here */

		/* Setting BulkLoadStatus Directory based on output Directory path */

		// String tempOutPutDir = getOutputDir();

		// this.outputDir = tempOutPutDir + "/" + getHTbale() + "/" +
		// getFeedID() + "/HFiles";

		/*
		 * return "/idn/home/kgowd/HBASE/" + getHTbale() + "/" + getFeedID() +
		 * "/HFiles";
		 */

		// this.bulkLoadStatusDir = tempOutPutDir + "/"+ getHTbale() + "/" +
		// getFeedID() + "/LoadStatus";

		/* Setting TableLockFile Directory based on output Directory path */

		// this.tableLockFileDir = tempOutPutDir + "/" + getHTbale();

	}

	/*
	 * Always call this method before using ETLConfiguration objectThis method
	 * initializes the ETLConfiguration object.
	 */

	/* Initialize ETLConfiguration object instance. */
	public static void init() {

		config = new ETLConfiguration();

	}

	/* Changes ends here */

	/**
	 * 
	 * Gets the single instance of ETLConfiguration.
	 * 
	 * 
	 * 
	 * @return single instance of ETLConfiguration
	 */

	public static ETLConfiguration getInstance() {

		if (config == null) {

			config = new ETLConfiguration();

		}

		return config;

	}

	/**
	 * 
	 * Gets the h tbale.
	 * 
	 * 
	 * 
	 * @return the hTbale
	 */

	public String getHTbale() {

		return HTbale;

	}

	/**
	 * 
	 * Sets the hTable.
	 * 
	 * 
	 * 
	 * @param hTable
	 *            the hTable to set
	 */

	public void setHTbale(String hTbale) {

		HTbale = hTbale;

	}

	/**
	 * 
	 * Gets the feed id.
	 * 
	 * 
	 * 
	 * @return the feedID
	 */

	public int getFeedID() {

		return FeedID;

	}

	/**
	 * 
	 * Sets the feed id.
	 * 
	 * 
	 * 
	 * @param feedID
	 *            the feedID to set
	 */

	public void setFeedID(int feedID) {

		FeedID = feedID;

	}

	/**
	 * 
	 * Gets the header column mapping.
	 * 
	 * 
	 * 
	 * @return the headerColumnMapping
	 */

	public LinkedHashMap<String, String> getHeaderColumnMapping() {

		return HeaderColumnMapping;

	}

	/**
	 * 
	 * Sets the header column mapping.
	 * 
	 * 
	 * 
	 * @param headerColumnMapping
	 *            the headerColumnMapping to set
	 */

	public void setHeaderColumnMapping(
			LinkedHashMap<String, String> headerColumnMapping) {

		HeaderColumnMapping = headerColumnMapping;

	}

	/**
	 * 
	 * Gets the seperator.
	 * 
	 * 
	 * 
	 * @return the seperator
	 */

	public String getSeperator() {

		return Seperator;

	}

	/**
	 * 
	 * Sets the seperator.
	 * 
	 * 
	 * 
	 * @param seperator
	 *            the seperator to set
	 */

	public void setSeperator(String seperator) {

		Seperator = seperator;

	}

	/**
	 * 
	 * Gets the columns.
	 * 
	 * 
	 * 
	 * @return the columns
	 */

	public String getColumns() {

		return columns;

	}

	/**
	 * 
	 * Sets the columns.
	 * 
	 * 
	 * 
	 * @param columns
	 *            the columns to set
	 */

	public void setColumns(String columns) {

		this.columns = columns;

	}

	/**
	 * 
	 * Gets the headers.
	 * 
	 * 
	 * 
	 * @return the headers
	 */

	public String getHeaders() {

		return headers;

	}

	/**
	 * 
	 * Sets the headers.
	 * 
	 * 
	 * 
	 * @param headers
	 *            the headers to set
	 */

	public void setHeaders(String headers) {

		this.headers = headers;

	}

	/**
	 * 
	 * Gets the checks if is key array.
	 * 
	 * 
	 * 
	 * @return the isKeyArray
	 */

	public String getIsKeyArray() {

		return isKeyArray;

	}

	/**
	 * 
	 * Sets the checks if is key array.
	 * 
	 * 
	 * 
	 * @param isKeyArray
	 *            the isKeyArray to set
	 */

	public void setIsKeyArray(String isKeyArray) {

		this.isKeyArray = isKeyArray;

	}

	/**
	 * 
	 * Gets the rowkey format.
	 * 
	 * 
	 * 
	 * @return the rowkeyFormat
	 */

	public String getRowkeyFormat() {

		return rowkeyFormat;

	}

	/**
	 * 
	 * Sets the rowkey format.
	 * 
	 * 
	 * 
	 * @param rowkeyFormat
	 *            the rowkeyFormat to set
	 */

	public void setRowkeyFormat(String rowkeyFormat) {

		this.rowkeyFormat = rowkeyFormat;

	}

	/**
	 * 
	 * @return the hbaseTableID
	 */

	public int getHbaseTableID() {

		return HbaseTableID;

	}

	/**
	 * 
	 * @param hbaseTableID
	 *            the hbaseTableID to set
	 */

	public void setHbaseTableID(int hbaseTableID) {

		HbaseTableID = hbaseTableID;

	}

	/**
	 * 
	 * @return the localConfigFilePath
	 */

	public String getLocalConfigFilePath() {

		return localConfigFilePath;

	}

	/**
	 * 
	 * @param localConfigFilePath
	 *            the localConfigFilePath to set
	 */

	public void setLocalConfigFilePath(String localConfigFilePath) {

		this.localConfigFilePath = localConfigFilePath;

	}

	/**
	 * 
	 * @return the outputDir
	 */

	/*
	 * 
	 * public String getOutputDir() {
	 * 
	 * return
	 * "/mapr/aeana/hbase/staging_area/agunni/"+getHTbale()+"/"+getFeedID(
	 * )+"/HFiles";
	 * 
	 * }
	 * 
	 * 
	 * 
	 * public String getBulkLoadStatusDir() {
	 * 
	 * return
	 * "/mapr/aeana/hbase/staging_area/agunni/"+getHTbale()+"/"+getFeedID(
	 * )+"/LoadStatus";
	 * 
	 * }
	 * 
	 * 
	 * 
	 * public String getTableLockFileDir() {
	 * 
	 * return "/mapr/aeana/hbase/staging_area/agunni/"+getHTbale();
	 * 
	 * }
	 * 
	 * 
	 * 
	 * /**
	 * 
	 * @return the runFeedID
	 */

	/* Update starts here */
	public String getOutputDir() {

		/*
		 * return "/idn/home/kgowd/HBASE/" + getHTbale() + "/" + getFeedID() +
		 * "/HFiles";
		 */

		// return this.outputDir;
		return outputDir + "/" + getHTbale() + "/" + getFeedID() + "/HFiles";

	}

	public void setOutputDir(String outputDirPath) {
		this.outputDir = outputDirPath;
	}

	/* Update ends here */

	public String getBulkLoadStatusDir() {

		/*
		 * return "/idn/home/kgowd/HBASE/" + getHTbale() + "/" + getFeedID() +
		 * "/LoadStatus";
		 */
		// return this.bulkLoadStatusDir;
		return outputDir + "/" + getHTbale() + "/" + getFeedID()
				+ "/LoadStatus";

	}

	public String getTableLockFileDir() {

		/*
		 * return "/idn/home/kgowd/HBASE/" + getHTbale();
		 */

		// return this.tableLockFileDir;

		return outputDir + "/" + getHTbale();
	}

	public String getRunFeedID() {

		return runFeedID;

	}

	/**
	 * 
	 * @param runFeedID
	 *            the runFeedID to set
	 */

	public void setRunFeedID(String runFeedID) {

		this.runFeedID = runFeedID;

	}

	/**
	 * 
	 * @return the tableID
	 */

	public String getTableID() {

		return tableID;

	}

	/**
	 * 
	 * @param tableID
	 *            the tableID to set
	 */

	public void setTableID(String tableID) {

		this.tableID = tableID;

	}

	/* Changes starts here */
	/* Util Methods starts */

	/* This method gets foldernames as List. */
	public static ArrayList<String> getFolderNames(String controlFile)

	throws IOException {

		ArrayList<String> inputFileList = new ArrayList<String>();

		BufferedReader reader = new BufferedReader(new FileReader(new File(

		controlFile)));

		String line;

		while ((line = reader.readLine()) != null) {

			inputFileList.add(line.replace("/mapr_ana/", "/mapr/"));

		}

		reader.close();

		return inputFileList;

	}

	/* This method gets individual folder name. */
	public static String getFolderName(String path) {

		System.out.println(path);

		String folderName = path.substring(0, path.lastIndexOf("/"));

		folderName = folderName.substring(folderName.lastIndexOf("/") + 1);

		return folderName;

	}

	/* Util Methods ends */
	/* Changes ends here */
}
---------------------------------------------------------------------------------------------------
package com.amex.warehouse.configuration;

// TODO: Auto-generated Javadoc
/**
 * The Class PDUploadConfigSingleton.
 *
 * @author Himanshu Prabhakar - AMEX AET
 * @version 0.1
 */
public class PDUploadConfigSingleton {

	/** The my instance. */
	private static PDUploadConfigSingleton myInstance = null;

	/** The data source denotion. */
	String dataSourceDenotion; /* A - Adobe */

	/** The trait denotion. */
	String traitDenotion; /* T */

	/** The segment denotion. */
	String segmentDenotion; /* S */

	/** The row key date format. */
	String rowKeyDateFormat; /* "yyyyMMdd" */

	/** The value date format. */
	private String valueDateFormat; /* "yyyyMMdd" */

	/** The input record date format. */
	String inputRecordDateFormat; /* "yyyy-MM-dd,HH:mm:ss" */

	/** The input data total fields. */
	Integer inputDataTotalFields; /* 5 */

	/**
	 * Instantiates a new pD upload config singleton.
	 */
	private PDUploadConfigSingleton() {
		dataSourceDenotion = new String("");
		traitDenotion = new String("");
		segmentDenotion = new String("");
		rowKeyDateFormat = new String("yyMMdd"); // Hard coded for the demo.
		setValueDateFormat(new String("yyMMdd-HHmmss"));
		inputRecordDateFormat = new String("yyyy-MM-dd HH:mm:ss"); // Hard coded
																	// for the
																	// demo.
		inputDataTotalFields = new Integer(4); // Hard coded for the demo.
	}

	/**
	 * Gets the single instance of PDUploadConfigSingleton.
	 *
	 * @return single instance of PDUploadConfigSingleton
	 */
	public static PDUploadConfigSingleton getInstance() {
		if (myInstance == null) {
			myInstance = new PDUploadConfigSingleton();
		}
		return myInstance;
	}

	/**
	 * Gets the data source denotion.
	 *
	 * @return the data source denotion
	 */
	public String getDataSourceDenotion() {
		return dataSourceDenotion;
	}

	/**
	 * Sets the data source denotion.
	 *
	 * @param dataSourceDenotion
	 *            the new data source denotion
	 */
	public void setDataSourceDenotion(String dataSourceDenotion) {
		this.dataSourceDenotion = dataSourceDenotion;
	}

	/**
	 * Gets the trait denotion.
	 *
	 * @return the trait denotion
	 */
	public String getTraitDenotion() {
		return traitDenotion;
	}

	/**
	 * Sets the trait denotion.
	 *
	 * @param traitDenotion
	 *            the new trait denotion
	 */
	public void setTraitDenotion(String traitDenotion) {
		this.traitDenotion = traitDenotion;
	}

	/**
	 * Gets the segment denotion.
	 *
	 * @return the segment denotion
	 */
	public String getSegmentDenotion() {
		return segmentDenotion;
	}

	/**
	 * Sets the segment denotion.
	 *
	 * @param segmentDenotion
	 *            the new segment denotion
	 */
	public void setSegmentDenotion(String segmentDenotion) {
		this.segmentDenotion = segmentDenotion;
	}

	/**
	 * Gets the row key date format.
	 *
	 * @return the row key date format
	 */
	public String getRowKeyDateFormat() {
		return rowKeyDateFormat;
	}

	/**
	 * Sets the row key date format.
	 *
	 * @param rowKeyDateFormat
	 *            the new row key date format
	 */
	public void setRowKeyDateFormat(String rowKeyDateFormat) {
		this.rowKeyDateFormat = rowKeyDateFormat;
	}

	/**
	 * Gets the input data total fields.
	 *
	 * @return the input data total fields
	 */
	public Integer getInputDataTotalFields() {
		return inputDataTotalFields;
	}

	/**
	 * Sets the input data total fields.
	 *
	 * @param inputDataTotalFields
	 *            the new input data total fields
	 */
	public void setInputDataTotalFields(Integer inputDataTotalFields) {
		this.inputDataTotalFields = inputDataTotalFields;
	}

	/**
	 * Gets the input record date format.
	 *
	 * @return the input record date format
	 */
	public String getInputRecordDateFormat() {
		return inputRecordDateFormat;
	}

	/**
	 * Sets the input record date format.
	 *
	 * @param inputRecordDateFormat
	 *            the new input record date format
	 */
	public void setInputRecordDateFormat(String inputRecordDateFormat) {
		this.inputRecordDateFormat = inputRecordDateFormat;
	}

	/**
	 * Gets the value date format.
	 *
	 * @return the value date format
	 */
	public String getValueDateFormat() {
		return valueDateFormat;
	}

	/**
	 * Sets the value date format.
	 *
	 * @param valueDateFormat
	 *            the new value date format
	 */
	public void setValueDateFormat(String valueDateFormat) {
		this.valueDateFormat = valueDateFormat;
	}

}
-----------------------------------------------------------------------------
package com.amex.warehouse.etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.amex.warehouse.configuration.ETLConfiguration;

// TODO: Auto-generated Javadoc

/**
 * 
 * The Class AutomationScheduler.
 */

public class AutomationScheduler {

	/** The logger. */

	public static Logger logger = Logger.getLogger(AutomationScheduler.class);

	/**
	 * 
	 * The main method.
	 * 
	 * 
	 * 
	 * @param args
	 *            the arguments
	 * 
	 * @throws Exception
	 *             the exception
	 */

	/*
	 * This is starting point of the project main will call run method of this
	 * class
	 * 
	 * Usage :<feedid> <runid> <stepid> <controlfilename> <controlfiledate>
	 * <tableid> <tablename> <QueueName> OR Usage :<Path to control file>
	 * <ConfigFile> <QueueName>
	 */

	public static void main(String args[]) throws Exception {

		int returnCode = run(args);

		System.out.println(returnCode);

		System.exit(returnCode);

	}

	/**
	 * 
	 * Run.
	 * 
	 * 
	 * 
	 * @param args
	 *            the args
	 * 
	 * @throws Exception
	 *             the exception
	 */

	/*
	 * This method initializes ETLConfiguration instance based on passed
	 * arguments.
	 * 
	 * If 9 arguments are passed while running this program
	 * then,ETLConfiguration will be initialized directly and also we have to
	 * pass control file path as args[3].
	 * 
	 * If 3 arguments are passed while running this program then,
	 * ETLConfiguration will be initialized from a configuration file that we
	 * have passed as args[0](control file).
	 * 
	 * It will call Schedular's Schedule Method and the flow of program will be
	 * forwarded to schedule method.
	 */
	public static int run(String args[]) throws Exception {

		ETLConfiguration etlConfig = ETLConfiguration.getInstance();

		/* Changes starts here */

		String queueName = null;

		if (etlConfig == null) {

			ETLConfiguration.init();

			etlConfig = ETLConfiguration.getInstance();

		}

		/* Changes ends here */

		String controlFile = null;

		// Initializing ETLConfiguration from command line arguments and control
		// file.
		if (args.length == 8) {

			etlConfig.setFeedID(Integer.parseInt(args[0]));

			etlConfig.setHTbale(args[6]);

			etlConfig.setHbaseTableID(Integer.parseInt(args[5]));

			etlConfig.setTableID(args[5]);

			etlConfig.setRunFeedID(args[1]);

			controlFile = args[3];

			/* Changes starts here */

			queueName = args[7];// Changed here for passing QueueName

			/* Changes ends here */

			System.out.println(args[0]);
			System.out.println(args[6]);

		}
		// Initializing ETLConfiguration from control file control file name is
		// passed as args[0] if running with 3 args.
		else if (args.length == 3) {

			etlConfig.setLocalConfigFilePath(args[1]);

			controlFile = args[0];

			/* Changes starts here */

			queueName = args[2];

			/* Changes ends here */

		} else {

			/* Update starts here */

			System.err
					.println("Usage :<feedid> <runid> <stepid> <controlfilename> <controlfiledate> <tableid> <tablename> <QueueName>");

			System.err.println("OR");

			System.err
					.println("Usage :<Path to control file> <ConfigFile> <QueueName> ");// Changed
																						// Here
																						// to
																						// Pass
																						// queueName
																						// as
																						// Argument

			System.exit(1);

			/* Update ends here */

		}

		logger.info("Abhishek's scheduler !");

		// Gets input file paths from control file
		ArrayList<String> inputFilePaths = getFilePaths(controlFile);

		// Retriving individual folder from each inputFilePath
		for (String input : inputFilePaths) {

			System.out.println(input + " => " + getFolderName(input));

		}

		/* Update starts here */

		int response = Scheduler.schedule(inputFilePaths, queueName);// Added an
																		// argument
																		// queueName.

		/* Update ends here */

		return response;

	}

	/**
	 * 
	 * Gets the file paths.
	 * 
	 * 
	 * 
	 * @param controlFile
	 *            the control file
	 * 
	 * @return the file paths
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */

	/*
	 * This method reads control file line by line and replaces /mapr_ana/ with
	 * /mapr/ andadds it to inputFile list.
	 */
	public static ArrayList<String> getFilePaths(String controlFile)
			throws IOException {

		ArrayList<String> inputFileList = new ArrayList<String>();

		BufferedReader reader = new BufferedReader(new FileReader(new File(
				controlFile)));

		String line;

		while ((line = reader.readLine()) != null) {

			inputFileList.add(line.replace("/mapr_ana/", "/mapr/"));

		}

		/* Changes starts here */

		reader.close();

		/* Changes ends here */

		return inputFileList;

	}

	/**
	 * 
	 * Gets the folder name.
	 * 
	 * 
	 * 
	 * @param path
	 *            the path
	 * 
	 * @return the folder name
	 */
	/* This method parses folder name from path argument and returns foldername. */
	public static String getFolderName(String path) {

		System.out.println(path);

		String folderName = path.substring(0, path.lastIndexOf("/"));

		folderName = folderName.substring(folderName.lastIndexOf("/") + 1);

		return folderName;

	}

}
-----------------------------------------------------------------------------------------
package com.amex.warehouse.etl;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.aexp.warehouse.exception.TableLockDetectedException;
import com.amex.warehouse.configuration.ETLConfiguration;
import com.amex.warehouse.helper.BulkLoad;
import com.amex.warehouse.helper.DBAccess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

// TODO: Auto-generated Javadoc
/**
 * The Class AAMLogLoadMR.
 */
public class ETLAAMDriver extends Configured implements Tool {

	/**
	 * The Enum MyCounter.
	 */
	public enum MyCounter {

		/** Number of Input records. */
		InputRecords,

		/** Number of Invalid records. */
		InvalidRecords,

		/** Number of Records inserted. */
		RecordsInserted,

		/** Number of records with Invalid key format. */
		InvalidKeyFormatRecords
	}

	private static FileOutputStream pidFileOutput = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) {

		try {

			System.out.println("Inside " + this.getClass().getName());

			ETLConfiguration etlConfig = ETLConfiguration.getInstance();
			Configuration conf = getConf();
			long milliSeconds = 1000 * 60 * 60;
			conf.setLong("mapred.task.timeout", milliSeconds);
			conf.set("mapreduce.child.java.opts", "-Xmx1g");
			conf.set("mapred.job.queue.name", "AETPMC");

			// Adding ETL properties to configuration

			int FeedID = etlConfig.getFeedID();

			conf.set("Seperator", etlConfig.getSeperator());
			conf.set("Headers", etlConfig.getHeaders());
			conf.set("ColumnMappings", etlConfig.getColumns());
			conf.set("isKeyArray", etlConfig.getIsKeyArray());
			conf.set("rowkeyFormat", etlConfig.getRowkeyFormat());

			// Printing job configuration

			System.out
					.println("*******************************************************************************");
			System.out.println("\t\t\tJob Configuration");
			System.out
					.println("-------------------------------------------------------------------------------");
			System.out.println("Seperator      :" + etlConfig.getSeperator());
			System.out.println("Headers        :" + etlConfig.getHeaders());
			System.out.println("ColumnMappings :" + etlConfig.getColumns());
			System.out.println("isKeyArray     :" + etlConfig.getIsKeyArray());
			System.out
					.println("rowkeyFormat   :" + etlConfig.getRowkeyFormat());
			System.out.println("HBase Table    :" + etlConfig.getHTbale());
			System.out.println("Feed ID        :" + FeedID);
			System.out.println("");
			System.out.println("Input Path     :" + args[1]);
			System.out.println("Output Path    :" + args[2]);
			System.out
					.println("*******************************************************************************");

			// Setting up directories

			FileSystem fs = FileSystem.get(conf);

			if (!fs.exists(new Path(etlConfig.getTableLockFileDir()))) {
				fs.mkdirs(new Path(etlConfig.getTableLockFileDir()));
			}

			// Creating a lock file at table level

			String pidLong = ManagementFactory.getRuntimeMXBean().getName();
			String[] items = pidLong.split("@");
			String pid = items[0];
			String tableLockFilePath = etlConfig.getTableLockFileDir() + "/"
					+ etlConfig.getTableID() + "_" + etlConfig.getRunFeedID()
					+ ".txt";
			File file = new File(tableLockFilePath);
			System.out.println(tableLockFilePath + ":" + file.getPath());
			FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
			FileLock lock = null;

			lock = channel.tryLock(0, 0, false);
			if (lock != null) {
				pidFileOutput = new FileOutputStream(file);
				pidFileOutput.write(pid.getBytes());

				pidFileOutput
						.write("\n*******************************************************************************"
								.getBytes());
				pidFileOutput.write("\n\t\t\tJob Configuration".getBytes());
				pidFileOutput
						.write("\n-------------------------------------------------------------------------------"
								.getBytes());
				pidFileOutput.write(("\nSeperator      :" + etlConfig
						.getSeperator()).getBytes());
				pidFileOutput.write(("\nHeaders        :" + etlConfig
						.getHeaders()).getBytes());
				pidFileOutput.write(("\nColumnMappings :" + etlConfig
						.getColumns()).getBytes());
				pidFileOutput.write(("\nisKeyArray     :" + etlConfig
						.getIsKeyArray()).getBytes());
				pidFileOutput.write(("\nrowkeyFormat   :" + etlConfig
						.getRowkeyFormat()).getBytes());
				pidFileOutput.write(("\nHBase Table    :" + etlConfig
						.getHTbale()).getBytes());
				pidFileOutput.write(("\nFeed ID        :" + FeedID).getBytes());
				pidFileOutput.write(("\n").getBytes());
				pidFileOutput
						.write(("\nInput Path     :" + args[1]).getBytes());
				pidFileOutput
						.write(("\nOutput Path    :" + args[2]).getBytes());
				pidFileOutput
						.write(("\n*******************************************************************************")
								.getBytes());
				pidFileOutput.flush();
				System.out.println("Lock is created on this process.");
			} else {
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String previousProcessID = reader.readLine();
				throw new TableLockDetectedException(previousProcessID);
			}

			Job job = new Job(conf);

			job.setJarByClass(ETLAAMDriver.class);
			job.setJobName(args[0]);
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapperClass(ETLMapper.class);

			FileInputFormat.addInputPaths(job, args[1]);

			FileSystem.getLocal(getConf()).delete(new Path(args[2]), true);
			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			job.setMapOutputValueClass(Put.class);
			job.setReducerClass(PutSortReducer.class);

			job.setWorkingDirectory(new Path(args[2] + "/../tmp"));
			HFileOutputFormat.configureIncrementalLoad(job, new HTable(conf,
					etlConfig.getHTbale()));

			SimpleDateFormat dateFormat = new SimpleDateFormat(
					"yyyy-MM-dd hh:mm:ss");
			String startTime = dateFormat.format(new Date());

			job.waitForCompletion(true);

			Counters counter = job.getCounters();

			long inputRecords = counter.findCounter(
					ETLAAMDriver.MyCounter.InputRecords).getValue();
			long invalidRecords = counter.findCounter(
					ETLAAMDriver.MyCounter.InvalidRecords).getValue();
			long recordsInserted = counter.findCounter(
					ETLAAMDriver.MyCounter.RecordsInserted).getValue();

			if (job.isSuccessful()) {
				// fs.delete(new Path(file.getPath()), true);
				fs.mkdirs(new Path(etlConfig.getBulkLoadStatusDir()));
				fs.createNewFile(new Path(etlConfig.getBulkLoadStatusDir()
						+ "/status.hfiles"));

				// TODO
				// Need to update the database change for insertion
				// DBAccess dba = new DBAccess();
				// int recordId =
				// dba.insertInfoIntoDB("AAM Logs Load","DIGITAL","INBOUND",new
				// SimpleDateFormat("yyyy-MM-dd").format(new
				// Date()),inputRecords+"",recordsInserted+"",invalidRecords+"","1",startTime,dateFormat.format(new
				// Date()),"HFile Generated","INBOUND",1,"");
				BulkLoad.load(args[2], etlConfig.getHTbale());
				// dba.updateBulkLoadStatus(recordId);
				fs.rename(new Path(etlConfig.getBulkLoadStatusDir()
						+ "/status.hfiles"),
						new Path(etlConfig.getBulkLoadStatusDir()
								+ "/status.load"));
				return 0;
			} else {
				return -1;
			}
		} catch (NoServerForRegionException e) {
			e.printStackTrace();
			return 1;
		} catch (SQLException e) {
			e.printStackTrace();
			return 1;
		} catch (IOException e) {
			e.printStackTrace();
			return 1;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return 1;
		} catch (TableLockDetectedException e) {
			e.printStackTrace();
			return 40;
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}

	}

}
-------------------------------------------------------------------------------
package com.amex.warehouse.etl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.aexp.warehouse.exception.InvalidPDRecordException;
import com.amex.warehouse.configuration.PDUploadConfigSingleton;
import com.amex.warehouse.etl.ETLAAMDriver.MyCounter;

// TODO: Auto-generated Javadoc

/**
 * 
 * The Class AAMLogMapper.
 */

public class ETLMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	/** The seperator. */

	private String seperator;

	/** The header length. */

	private int headerLength;

	/** The headers. */

	private String[] headers;

	/** The column mapping. */

	private String[] columnMapping;

	/** The rowkey format. */

	private String rowkeyFormat;

	/** The key indexes. */

	private ArrayList<String> keyIndexes;

	/** The column indexes. */

	private ArrayList<String> columnIndexes;

	/** The header value map. */

	private Map<String, String> headerValueMap;

	/** The header column map. */

	private Map<String, String> headerColumnMap;

	/** The my context. */

	private static Context myContext;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
	 * Mapper.Context)
	 */

	public void setup(Context context) {

		myContext = context;

		keyIndexes = new ArrayList();

		columnIndexes = new ArrayList();

		headerValueMap = new HashMap<String, String>();

		headerColumnMap = new HashMap<String, String>();

		Configuration conf = context.getConfiguration();

		seperator = conf.get("Seperator");

		headers = conf.get("Headers").split(",");

		columnMapping = conf.get("ColumnMappings").split(",");

		rowkeyFormat = conf.get("rowkeyFormat");

		for (int i = 0; i < headers.length; i++) {

			headerColumnMap.put(headers[i], columnMapping[i]);

		}

		int i = 0;

		for (String isKey : conf.get("isKeyArray").split(",")) {

			if (isKey.equalsIgnoreCase("Y")) {

				keyIndexes.add(headers[i]);

			} else {

				columnIndexes.add(headers[i]);

			}

			i++;

		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		context.getCounter(MyCounter.InputRecords).increment(1);

		String[] values = value.toString().split(seperator, -1);

		if (values.length != headers.length) {

			context.getCounter(MyCounter.InvalidRecords).increment(1);

			context.setStatus("Values Length : " + values.length
					+ " Headers Length : " + headers.length);

		} else {

			for (int i = 0; i < headers.length; i++) {

				headerValueMap.put(headers[i], values[i]);

			}

			// String cookieRow = "AAM-" +values[1]+"-"+getValueDate(values[0]);

			String cookieRow = null;

			try {

				cookieRow = getRowKey(rowkeyFormat, keyIndexes, headerValueMap);

			} catch (Exception e) {

				context.getCounter(
						ETLAAMDriver.MyCounter.InvalidKeyFormatRecords)
						.increment(1);

			}

			if (cookieRow != null) {

				ImmutableBytesWritable recordKey = new ImmutableBytesWritable();

				recordKey.set(Bytes.toBytes(cookieRow));

				Put put = new Put(Bytes.toBytes(cookieRow));

				for (String column : columnIndexes) {

					String[] familyColumn = headerColumnMap.get(column).split(
							"~");

					String columnFamily = familyColumn[0];

					String columnName = familyColumn[1];

					String columnValue = headerValueMap.get(column);

					/* Changes starts here */

					columnValue = parseColumnValue(columnValue);

					/* Changes ends here */

					context.setStatus(cookieRow + "\t" + columnFamily + ":"
							+ columnName + "=>" + columnValue);

					put.add(columnFamily.getBytes(), columnName.getBytes(),
							columnValue.getBytes());

				}

				context.getCounter(ETLAAMDriver.MyCounter.RecordsInserted)
						.increment(1);

				context.write(recordKey, put);

			}

		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce
	 * .Mapper.Context)
	 */

	public void cleanup(Context context) {

		Configuration conf = context.getConfiguration();

		conf.set("Completion", "Success");

	}

	/**
	 * 
	 * Gets the value date.
	 * 
	 * 
	 * 
	 * @param recordDateTimeStamp
	 *            the record date time stamp
	 * 
	 * @return the value date
	 */

	public static String getValueDate(String recordDateTimeStamp) {

		SimpleDateFormat outputFormat = new SimpleDateFormat(
				PDUploadConfigSingleton.getInstance().getValueDateFormat());

		final List<String> dateFormats = Arrays.asList("yyyy-MM-dd HH:mm:ss",
				"yyyy-MM-dd,HH:mm:ss");

		for (String format : dateFormats) {

			SimpleDateFormat sdf = new SimpleDateFormat(format);

			try {

				if (sdf.parse(recordDateTimeStamp) != null) {

					return outputFormat.format(sdf.parse(recordDateTimeStamp));

				}

			} catch (ParseException e) {

				// intentionally empty

			}

		}

		throw new IllegalArgumentException("Invalid input for date. Given '" +

		recordDateTimeStamp +

		"', expecting format yyyy-MM-dd HH:mm:ss or yyyy-MM-dd,HH:mm:ss");

	}

	/**
	 * 
	 * Gets the format.
	 * 
	 * 
	 * 
	 * @param rowkeyFormat
	 *            the rowkey format
	 * 
	 * @param header
	 *            the header
	 * 
	 * @return the format
	 */

	private static String getFormat(String rowkeyFormat, String header) {

		return rowkeyFormat.substring(rowkeyFormat.indexOf(header + "[")
				+ (header + "[").length(),
				rowkeyFormat.indexOf("]", rowkeyFormat.indexOf(header)));

	}

	/**
	 * 
	 * Gets the value replaced with format.
	 * 
	 * 
	 * 
	 * @param rowkeyFormat
	 *            the rowkey format
	 * 
	 * @param header
	 *            the header
	 * 
	 * @param headerValueMap
	 *            the header value map
	 * 
	 * @return the value replaced with format
	 * 
	 * @throws Exception
	 *             the exception
	 */

	private static String getValueReplacedWithFormat(String rowkeyFormat,
			String header, Map<String, String> headerValueMap) throws Exception {
		long reverseTimestamp1 = Long.MAX_VALUE - System.currentTimeMillis();
		String format = getFormat(rowkeyFormat, header);

		String[] formatAttributes = format.split("~");

		String rowKeyValueToBeReplaced = "";

		if (formatAttributes.length == 3) {

			if (formatAttributes[0].equalsIgnoreCase("DATE")) {

				Date inputDate;

				if (formatAttributes[1].equalsIgnoreCase("SYSDATE")) {

					inputDate = new Date();

				} else {

					inputDate = new SimpleDateFormat(formatAttributes[1])
							.parse(headerValueMap.get(header));

				}

				rowKeyValueToBeReplaced = new SimpleDateFormat(
						formatAttributes[2]).format(inputDate);

			} else if (formatAttributes[0].equalsIgnoreCase("Length")) {
				Integer len = Integer.parseInt(formatAttributes[1]);
				String value = headerValueMap.get(header);
				rowKeyValueToBeReplaced = formatAttributes[2];
				if (value.length() >= len) {
					rowKeyValueToBeReplaced = value;
				} else {
					rowKeyValueToBeReplaced = rowKeyValueToBeReplaced
							.substring(0, len - value.length())
							+ value + "." + reverseTimestamp1;
				}
			}

		} else {

			throw new Exception(); // RowKey format Exception

		}

		return rowkeyFormat.replace("<" + header + "[" + format + "]>",
				rowKeyValueToBeReplaced);

	}

	/**
	 * 
	 * Gets the row key.
	 * 
	 * 
	 * 
	 * @param rowkeyFormat
	 *            the rowkey format
	 * 
	 * @param headersInRowKey
	 *            the headers in row key
	 * 
	 * @param headerValueMap
	 *            the header value map
	 * 
	 * @return the row key
	 * 
	 * @throws Exception
	 *             the exception
	 */

	private static String getRowKey(String rowkeyFormat,
			List<String> headersInRowKey, Map<String, String> headerValueMap)
			throws Exception {

		/* Changes starts here */

		long reverseTimestamp = Long.MAX_VALUE - System.currentTimeMillis();

		/* Changes ends here */

		String inpuRowKeyFormat = rowkeyFormat;

		ArrayList<String> variablesInKey = getVarsinRowKey(inpuRowKeyFormat);

		for (String header : headersInRowKey) {

			if (inpuRowKeyFormat.contains(header)) {

				if (inpuRowKeyFormat.contains(header + "[")
						&& headerValueMap.containsKey(header)) {

					inpuRowKeyFormat = getValueReplacedWithFormat(
							inpuRowKeyFormat, header, headerValueMap);

				} else {

					inpuRowKeyFormat = inpuRowKeyFormat.replace("<" + header
							+ ">", headerValueMap.get(header));

				}

			} else {

				if (getFormat(rowkeyFormat, header).split("~")[0]
						.equalsIgnoreCase("DATE")) {

					FileSplit fileSplit = (FileSplit) myContext.getInputSplit();

					FileSystem fs = FileSystem
							.get(myContext.getConfiguration());

					FileStatus status = fs.getFileStatus(fileSplit.getPath());

					String timeStamp = new SimpleDateFormat("yyMMdd-HHmmss")
							.format(new Date(status.getModificationTime()));

					inpuRowKeyFormat = rowkeyFormat
							.replace(
									"<" + header + "["
											+ getFormat(rowkeyFormat, header)
											+ "]>", timeStamp);
					;

					// inpuRowKeyFormat =
					// rowkeyFormat.replace("<"+header+"["+getFormat(rowkeyFormat,
					// header)+"]>", reverseTimestamp+"");

				}

				/* Changes starts here */
				else if (getFormat(rowkeyFormat, header).split("~")[0]
						.equalsIgnoreCase("RDATE")) {

					FileSplit fileSplit = (FileSplit) myContext.getInputSplit();

					FileSystem fs = FileSystem
							.get(myContext.getConfiguration());

					FileStatus status = fs.getFileStatus(fileSplit.getPath());

					String timeStamp = new SimpleDateFormat("yyMMdd-HHmmss")
							.format(new Date(status.getModificationTime()));

					inpuRowKeyFormat = rowkeyFormat.replace("<" + header + "["
							+ getFormat(rowkeyFormat, header) + "]>",
							reverseTimestamp + "");

				}
				/* Changes ends here */
				else {

					throw new Exception(); // RowKey format Exception

				}

			}

			variablesInKey.remove(header);

		}

		for (String header : variablesInKey) {

			if (inpuRowKeyFormat.contains(header)) {

				if (inpuRowKeyFormat.contains(header + "[")) {

					if (getFormat(rowkeyFormat, header).split("~")[0]
							.equalsIgnoreCase("DATE")) {

						FileSplit fileSplit = (FileSplit) myContext
								.getInputSplit();

						FileSystem fs = FileSystem.get(myContext
								.getConfiguration());

						FileStatus status = fs.getFileStatus(fileSplit
								.getPath());

						String timeStamp = new SimpleDateFormat("yyMMdd-HHmmss")
								.format(new Date(status.getModificationTime()));

						inpuRowKeyFormat = inpuRowKeyFormat.replace(
								"<" + header + "["
										+ getFormat(inpuRowKeyFormat, header)
										+ "]>", timeStamp);

					}
					/* Changes starts here */
					else if (getFormat(rowkeyFormat, header).split("~")[0]
							.equalsIgnoreCase("RDATE")) {

						FileSplit fileSplit = (FileSplit) myContext
								.getInputSplit();

						FileSystem fs = FileSystem.get(myContext
								.getConfiguration());

						FileStatus status = fs.getFileStatus(fileSplit
								.getPath());

						String timeStamp = new SimpleDateFormat("yyMMdd-HHmmss")
								.format(new Date(status.getModificationTime()));

						inpuRowKeyFormat = inpuRowKeyFormat.replace(
								"<" + header + "["
										+ getFormat(inpuRowKeyFormat, header)
										+ "]>", reverseTimestamp + "");

					}

					/* Changes ends here */
					else {

						throw new Exception(); // RowKey format Exception

					}

				} else {

					throw new Exception(); // RowKey format Exception

				}

			}

		}

		return inpuRowKeyFormat;

	}

	/**
	 * 
	 * Gets the varsin row key.
	 * 
	 * 
	 * 
	 * @param rowkeyFormat
	 *            the rowkey format
	 * 
	 * @return the varsin row key
	 */

	private static ArrayList<String> getVarsinRowKey(String rowkeyFormat) {

		String inpuRowKeyFormat = rowkeyFormat;

		ArrayList<String> variablesList = new ArrayList<String>();

		if (rowkeyFormat.contains("<") && rowkeyFormat.contains(">")) {

			while (inpuRowKeyFormat.contains("<")
					|| inpuRowKeyFormat.contains(">")) {

				int beginIndex = inpuRowKeyFormat.indexOf("<");

				int endIndex = inpuRowKeyFormat.indexOf(">");

				String variableExtract = inpuRowKeyFormat.substring(
						beginIndex + 1, endIndex);

				if (variableExtract.contains("[")) {

					variableExtract = variableExtract.substring(0,
							variableExtract.indexOf("["));

				} else {

					System.out
							.println("No format specied, variable will be use as is.");

				}

				variablesList.add(variableExtract);

				inpuRowKeyFormat = inpuRowKeyFormat.substring(endIndex + 1);

			}

		} else {

			System.out.println("Rowkey doesnt have variables");

		}

		return variablesList;

	}

	
	/* Changes starts here */

	/*
	 * remove single quotes and double quotes from string value.
	 */
	private static String parseColumnValue(String value) {

		try {

			if (value.contains("'")) {

				value = value.replace('\'', ' ');

			}
			if (value.contains("\"")) {

				value = value.replace('\"', ' ');

			}

		} catch (Exception exception) {

			exception.printStackTrace();

		}

		return value;

	}

	/* Changes ends here */

}
-------------------------------------------------------------------------------------
package com.amex.warehouse.etl;

import java.io.IOException;

import java.sql.SQLException;

import java.text.SimpleDateFormat;

import java.util.ArrayList;

import java.util.Date;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.util.ToolRunner;

import com.aexp.warehouse.exception.TableLockDetectedException;

import com.amex.warehouse.configuration.DataLoadProperties;

import com.amex.warehouse.configuration.ETLConfiguration;

import com.amex.warehouse.helper.DBHelperETL;

import com.amex.warehouse.helper.DBHelperETLNew;

import com.amex.warehouse.helper.ETLConfigBuilder;

import com.amex.warehouse.helper.FileTypeHelper;

// TODO: Auto-generated Javadoc

/**
 * 
 * The Class Scheduler.
 */

public class Scheduler {

	/** The Constant AAM_WAREHOUSE_JOB_PREFIX. */

	public static final String AAM_WAREHOUSE_JOB_PREFIX = "Digital AAM WareHouse";

	/** The Constant OMNITURE_WAREHOUSE_JOB_PREFIX. */

	public static final String OMNITURE_WAREHOUSE_JOB_PREFIX = "Digital Omniture WareHouse";

	/** The Constant JOB_TRACKER_URL. */

	public static final String JOB_TRACKER_URL = "10.74.114.138:9001";

	/** The date. */

	private static String date = new SimpleDateFormat("yyyyMMdd")
			.format(new Date());

	/**
	 * 
	 * Schedule.
	 * 
	 * 
	 * 
	 * @param jobType
	 *            the job type
	 * 
	 * @param inputPaths
	 *            the input paths
	 * 
	 * @param IOProps
	 *            the IO props
	 */

	/*
	 * This method is called from AutomationScheduler's run method. If we have
	 * passed 3 arguments(Passing ContorolFile) then it will build
	 * ETLConfiguration based on that LocalConfiguration paths. otherwise it
	 * loads parameters from Database using(DBHelperETLNew) and initializes
	 * ETLConfiguration. After initializing the ETLConfiguration it will call
	 * run method and pass ETLAAMDriver , so flow will be forwarded to
	 * ETLAAMDriver's run method and also we are passing arguments which will be
	 * initialized in ETLAAMDriver.
	 */
	public static int schedule(ArrayList<String> inputPaths, String queueName) {

		try {

			String outputPath, jobName = "";

			ETLConfiguration etlConfig = ETLConfiguration.getInstance();

			String inputPath = inputPaths.toString().replace("[", "")
					.replace("]", "").replaceAll(" ", "");

			DBHelperETLNew etlHelper = new DBHelperETLNew();

			ETLConfigBuilder configBuilder = new ETLConfigBuilder();

			// Here we will either get configuration from local config file OR
			// Database
			if (etlConfig.getLocalConfigFilePath() != null) {

				// Loading and building configuration from Config File
				configBuilder
						.buildETLConfig(etlConfig.getLocalConfigFilePath());

			} else // Loading and building configuration from DB
			{

				etlHelper.buildETLConfig(etlConfig.getFeedID(),
						etlConfig.getHbaseTableID(), etlConfig.getHTbale());

			}

			outputPath = etlConfig.getOutputDir();

			jobName = etlConfig.getHTbale() + "-" + etlConfig.getFeedID()
					+ date;

			/* Update starts here */

			String[] arguments = { jobName, inputPath, outputPath, queueName }; // Changed
																				// here
																				// appended
																				// queueName

			/* Update ends here */

			System.out.println("Processing for : " + jobName);

			int response = ToolRunner.run(HBaseConfiguration.create(),
					new ETLAAMDriver(), arguments);

			return response;

		} catch (ClassNotFoundException e) {

			e.printStackTrace();

			return 1;

		} catch (SQLException e) {

			e.printStackTrace();

			return 1;

		} catch (IOException e) {

			e.printStackTrace();

			return 1;

		} catch (TableLockDetectedException e) {

			e.printStackTrace();

			return 40;

		} catch (Exception e) {

			e.printStackTrace();

			return 1;

		}

	}

}
-----------------------------------------------------------------
package com.amex.warehouse.helper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

// TODO: Auto-generated Javadoc
/**
 * The Class BulkLoad.
 */
public class BulkLoad {

	/**
	 * Load.
	 *
	 * @param pathToHFile
	 *            the path to h file
	 * @param HTable
	 *            the h table
	 * @throws Exception
	 *             the exception
	 */
	public static void load(String pathToHFile, String HTable) throws Exception {

		Configuration conf = new Configuration();

		long milliSeconds = 1000 * 60 * 60;
		conf.setLong("mapred.task.timeout", milliSeconds);
		conf.set("mapreduce.child.java.opts", "-Xmx1g");

		HBaseConfiguration.addHbaseResources(conf);

		LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(conf);

		HTable table = new HTable(conf, HTable);

		loadFfiles.doBulkLoad(new Path(pathToHFile), table);

		System.out.println("Done");
	}

}

---------------------------------------------------------------------
package com.amex.warehouse.helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Properties;

import com.amex.warehouse.configuration.ETLConfiguration;

public class DBHelperETLNew {

	/** The connect. */

	private static Connection connect = null;

	/** The statement. */

	private static Statement statement = null;

	/** The prepared statement. */

	public static PreparedStatement preparedStatement = null;

	/** The db name. */

	public static String dbName;

	/** The table name. */

	public static String tableName;

	/** The driver class. */

	public static String driverClass;

	/** The connection url. */

	public static String connectionURL;

	/** The user name. */

	public static String userName;

	/** The password. */

	public static String password;

	/** The data from my sql. */

	private static String[] dataFromMySQL;

	/**
	 * 
	 * Instantiates a new DB helper etl.
	 * 
	 *
	 * 
	 * @throws Exception
	 *             the exception
	 */

	public DBHelperETLNew() throws Exception {

		initializeConnection(null);

	}

	/**
	 * 
	 * Initialize connection.
	 * 
	 *
	 * 
	 * @param prop
	 *            the prop
	 * 
	 * @throws Exception
	 *             the exception
	 */

	public static void initializeConnection(Properties prop) throws Exception {

		dbName = "pmcdb";

		tableName = "data_request";

		String driverClass = "com.mysql.jdbc.Driver";

		String connectionURL = "jdbc:mysql://10.74.132.38:3306/pmcprod";

		// String connectionURL = "jdbc:mysql://139.61.246.133:3306/pmcdb";

		String userName = "pmcprod";

		String password = "pmcprod";

		Class.forName("com.mysql.jdbc.Driver");

		if (connect == null) {

			System.out
					.println("Connecting to ETL Database to read the configuration.");

			connect = DriverManager.getConnection(connectionURL + "?" + "user="

			+ userName + "&password=" + password);

		}

	}

	/**
	 * 
	 * Builds the etl config.
	 * 
	 *
	 * 
	 * @param controlFile
	 *            the control file
	 * 
	 * @param Category
	 *            the category
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	/*
	 * This method retrieves parameters from database and build
	 * ETLConfiguration.
	 */

	public void buildETLConfig(int FeedID, int HTableID, String HTableName)
			throws SQLException {

		ETLConfiguration config = ETLConfiguration.getInstance();

		ArrayList<String> fieldHeaders = new ArrayList<String>();

		ArrayList<String> columns = new ArrayList<String>();

		ArrayList<String> isKeyList = new ArrayList<String>();

		/*
		 * PreparedStatement preparedStatement =
		 * connect.prepareStatement("SELECT PFA.ColumnIndex `Index`, "
		 * 
		 * + "PFA.ColumnName HeaderName, "
		 * 
		 * + "PCM.OuputColFamily HBaseColumnFamily, "
		 * 
		 * + "PCM.OutputColName HbaseColumnName , "
		 * 
		 * + "PFI.FileDelimiter as Seperator, "
		 * 
		 * + "PFA.isRowKey as isKey, "
		 * 
		 * + "PHT.RowKey RowKeyFormat "
		 * 
		 * +
		 * "FROM   pmc.pmc_hbase_tablemap PHT,pmc.pmc_hbase_columnmap PCM,pmc.pmc_feed_attributes PFA,pmc.pmc_feed_inbound PFI "
		 * 
		 * + "WHERE  PHT.TblMapID = PCM.TblMapID "
		 * 
		 * + "AND    PCM.FeedID = PFA.FeedID "
		 * 
		 * + "AND    PCM.FeedID = PFI.FeedID "
		 * 
		 * + "AND    PHT.HbaseTableName = ? "
		 * 
		 * + "AND    PCM.FeedID = ? "
		 * 
		 * + "ORDER BY PFA.ColumnIndex ASC;");
		 */

		PreparedStatement preparedStatement = connect
				.prepareStatement("SELECT pfa.ColumnIndex,pfa.columnname,pftc.ColumnFamily,pftc.ColumnName,if(pf.Filedelimiter = '  ','TAB',pf.Filedelimiter) as delimiter,pfa.isPrimaryKey,pft.TableRowKey FROM   pmc_feed_attributes pfa, pmc_feed_table_column pftc,pmc_feed_table pft,pmc_feed pf WHERE  pfa.AttributeID = pftc.AttributeID AND    pftc.TableID    = pft.TableID AND    pfa.FeedID = pf.FeedID AND    pftc.TableID    = ? AND    pfa.FeedID      = ? ORDER BY pfa.ColumnIndex;");

		System.out.println(preparedStatement.toString());

		preparedStatement.setInt(1, HTableID);

		preparedStatement.setInt(2, FeedID);

		ResultSet rs = preparedStatement.executeQuery();

		config.setFeedID(FeedID);

		config.setHTbale(HTableName);

		LinkedHashMap HeaderColumnMapping = new LinkedHashMap<String, String>();

		while (rs.next()) {

			HeaderColumnMapping.put(rs.getString(2),
					rs.getString(3) + "~" + rs.getString(4));

			fieldHeaders.add(rs.getString(2));

			config.setSeperator(rs.getString(5));

			config.setRowkeyFormat(rs.getString(7));

			columns.add(rs.getString(3) + "~" + rs.getString(4));

			isKeyList.add(rs.getString(6));

		}

		config.setHeaderColumnMapping(HeaderColumnMapping);

		config.setHeaders(fieldHeaders.toString().replace(" ", "")
				.replace("[", "").replace("]", ""));

		config.setColumns(columns.toString().replace(" ", "").replace("[", "")
				.replace("]", ""));

		config.setIsKeyArray(isKeyList.toString().replace(" ", "")
				.replace("[", "").replace("]", ""));

	}

	/**
	 * 
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * 
	 * @throws Exception
	 *             the exception
	 */

	public static void main(String args[]) throws Exception {

		DBHelperETL jdbc = new DBHelperETL();

		// Changed Here to initialize OutPutDir BulkLoadStatusDir
		// TableLockFileDir of ETLConfiguration that user will have to give at
		// runtime..

		// System.out.println(jdbc.getFeedID("amexaam_logs_2014-07-14.txt.control"));

		// jdbc.buildETLConfig("", "");

		/* Update starts here */

		ETLConfiguration.init();

		ETLConfiguration config = ETLConfiguration.getInstance();

		System.out.println("Headers => " + config.getHeaders());

		System.out.println("Columns => " + config.getColumns());

		/* Update ends here */

	}

}

-------------------------------------------------------------
package com.amex.warehouse.helper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import sun.nio.cs.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.amex.warehouse.configuration.ETLConfiguration;

public class ETLConfigBuilder {

	// final static Charset ENCODING = StandardCharsets.class;

	public static void main(String[] args) throws IOException {

		ETLConfigBuilder build = new ETLConfigBuilder();

		/* Update starts here */

		// Initializing Directories..
		ETLConfiguration.init();

		build.buildETLConfig("config.txt");

		ETLConfiguration config = ETLConfiguration.getInstance();

		System.out.println("Feed ID => " + config.getFeedID());

		System.out.println("HTable  => " + config.getHTbale());

		System.out.println("Headers => " + config.getHeaders());

		System.out.println("Columns => " + config.getColumns());

		System.out.println("Is Key  => " + config.getIsKeyArray());

		System.out.println("Table ID  => " + config.getTableID());

		System.out.println("Run Feed ID  => " + config.getRunFeedID());

		System.out.println("Col Map => "
				+ config.getHeaderColumnMapping().toString());

		/* Update ends here */

	}

	public void buildETLConfig(String configFilePath) throws IOException {

		ETLConfiguration config = ETLConfiguration.getInstance();

		ArrayList<String> fieldHeaders = new ArrayList<String>();

		ArrayList<String> columns = new ArrayList<String>();

		ArrayList<String> isKeyList = new ArrayList<String>();

		LinkedHashMap HeaderColumnMapping = new LinkedHashMap<String, String>();

		BufferedReader reader = new BufferedReader(new FileReader(
				configFilePath));

		String line = null;

		int lineNumber = 0;

		boolean isHeadPresent = config.isHeadPresent();

		boolean isTailPresent = config.isTailPresent();

		/* Update starts here */
		ArrayList<String> linesList = new ArrayList<String>();

		int tailIndex = -1;

		try {
			BufferedReader tempReader = new BufferedReader(new FileReader(
					configFilePath));
			{
				linesList.add(line);

				tailIndex++;

			}
			reader.close();

		}

		catch (Exception exception)

		{
			if (isHeadPresent)

			{
				linesList.remove(0);// Removing First line from data file

			}
			if (isTailPresent) {
				// Both Head and Tail Present Head Removed hence decreasing
				// index..
				if (isHeadPresent) {
					linesList.remove(tailIndex - 1);// Removing Last line from
													// data file

				}
				// only Tail present
				else {
					linesList.remove(tailIndex);// Removing Last line from data
												// file
				}
			}
			int lineCount = 0;
			for (String tempLine : linesList) {
				if (!tempLine.startsWith("##") && tempLine.length() > 0) {
					if (lineCount == 0) {
						String[] jobParams = tempLine.split(",");

						config.setFeedID(Integer.parseInt(jobParams[0]));

						config.setHTbale(jobParams[1]);

						config.setSeperator(jobParams[2]);

						config.setRowkeyFormat(jobParams[3]);

						config.setTableID(jobParams[4]);

						config.setRunFeedID(jobParams[5]);

						lineNumber++;

					}

					else {

						String[] mappings = line.split("\t");

						HeaderColumnMapping.put(mappings[1], mappings[2] + "~"
								+ mappings[3]);

						fieldHeaders.add(mappings[1]);

						columns.add(mappings[2] + "~" + mappings[3]);

						isKeyList.add(mappings[4]);

						/*
						 * if(mappings[4].equalsIgnoreCase("Y")){
						 * 
						 * isKeyList.add(mappings[1]);
						 * 
						 * }
						 */
					}
				}

			}

			// writing File once Again for removing Head Tail..

			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(
					configFilePath));

			for (String templine : linesList) {
				bufferedWriter.write(templine);
				bufferedWriter.newLine();
			}

			bufferedWriter.close();

			/* Update ends here */

			reader.close();

			config.setHeaderColumnMapping(HeaderColumnMapping);

			config.setHeaders(fieldHeaders.toString().replace(" ", "")
					.replace("[", "").replace("]", ""));

			config.setColumns(columns.toString().replace(" ", "")
					.replace("[", "").replace("]", ""));

			config.setIsKeyArray(isKeyList.toString().replace(" ", "")
					.replace("[", "").replace("]", ""));

		}

	}
}
--------------------
package com.amex.warehouse.interfaces;

import org.apache.hadoop.conf.Configurable;

public interface AmexTool extends Configurable {

	int run(Object obj) throws Exception;

}
-------------------------------
package com.amex.warehouse.interfaces;

import org.apache.hadoop.conf.Configuration;

public class AmexToolRunner {

	public static int run(Configuration conf, AmexTool tool, Object obj)
			throws Exception {

		return tool.run(obj);
	}

}
---------------------------------------------
package com.amex.warehouse.junit;

import com.amex.warehouse.etl.AutomationScheduler;

public class AutomatedSchedulerJunit {

	public static void main(String[] args) {

		System.out
				.println(AutomationScheduler
						.getFolderName("/mapr/aeana/amexuserdata/AETPMC/agunni/Mapred/OMNITURE/enterprise_mr/enterprise_mr_20131123.dat"));

	}

}
------------------
