// Databricks notebook source
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.apache.spark.sql.functions._


// COMMAND ----------

import java.util. Properties
import java.sql.{Connection, DriverManager}

// COMMAND ----------

import spark.implicits._    
val sc = spark.sparkContext;    
sc.setLogLevel("ERROR")
val sqlContext = spark.sqlContext;
//AWS ID: Needs to be set as Environment variable
val aws_id = System.getenv("AWS_ID")
//AWS Secret key: Needs to be set as Environment variable
val aws_secret = System.getenv("AWS_SECRET")
//Set the aws id and secret key in spark hadoop configuration
sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", aws_id)    
sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", aws_secret)

// COMMAND ----------

//Analysis parameters
val INCIDENT_START_DATE = 1567900800;  //Unix Epoch time
val INCIDENT_END_DATE = 1568592000; //Unix Epoch time
val EVENT_INCIDENT_INTERVAL = 14400; //Specify interval in seconds

// COMMAND ----------

//Set database properties
//Set JDBC properties for writing to the database
val DB_USER = System.getenv("DB_USER");
val DB_PASS = System.getenv("DB_PASS");
val DB_HOST = System.getenv("DB_HOST");
val DB_NAME = System.getenv("DB_NAME");

//Table Names
val EVENT_TABLE_NAME = "event_incident_mapping";
val INCIDENT_TABLE_NAME = "incidents";

// COMMAND ----------

//Get the business and parent business ID into a dataframe
var bus_parent_df = spark.read.option("header", "true").option("ignoreLeadingWhiteSpace",true).csv("s3://navisite-dataset-test/bus_parent.csv");
//Rename the columns in dataframe
bus_parent_df = bus_parent_df.select(
            bus_parent_df.col("PARENT_ID").as("PARENT_BUS_ID"),
            bus_parent_df.col("BUS_ID").as("HIER_BUS_ID")
    );

// COMMAND ----------

//Get the device and customer data into a dataframe
var customer_df = spark.read.option("header", "true").option("ignoreLeadingWhiteSpace",true).csv("s3://navisite-dataset-test/customer.csv");
//Rename the columns in dataframe
customer_df = customer_df.select(
            customer_df.col("RECONCILIATION_ID"),
            customer_df.col("BUS_ID").as("BUS_ID")
    );

// COMMAND ----------

//Get the device and monitor details into a dataframe
var device_df = spark.read.option("header", "true").option("ignoreLeadingWhiteSpace",true).csv("s3://navisite-dataset-test/device.csv");
//Rename the columns in dataframe
device_df = device_df.select(
            device_df.col("TARGET_ID").as("DEVICE_TARGET_ID"),
            device_df.col("RECON_ID")
    )

// COMMAND ----------

//Get the device location and other details into a dataframe 
var device_location_df = spark.read.option("header", "true").option("ignoreLeadingWhiteSpace",true).csv("s3://navisite-dataset-test/device_location.csv");
//Rename the columns in dataframe
device_location_df = device_location_df.select( device_location_df.col("BUS_ID").as("BUS_ID_DEV"),
    device_location_df.col("DEVICE_TYPE"),
    device_location_df.col("OPERATING_SYSTEM"),
    device_location_df.col("RECONCILIATION_ID"),
    device_location_df.col("SITE"),
    device_location_df.col("SITE").as("SITE_DESC"),
    device_location_df.col("IS_VIRTUAL")
    );

// COMMAND ----------

/*
  Clean site name by removing special characters from the site name, for grouping.
  SITE column: Will have the reduced name
  SITE_DESC column: Will have real names
*/
device_location_df =  device_location_df.withColumn("SITE" ,regexp_replace(device_location_df.col("SITE"), "[^0-9a-zA-Z]+", ""));

// COMMAND ----------

//Get the parent-child monitor information  into a dataframe 
var parent_df = spark.read.option("header", "true").option("ignoreLeadingWhiteSpace",true).csv("s3://navisite-dataset-test/parent.csv");
//Rename the columns in dataframe
parent_df = parent_df.select(
            parent_df.col("PARENT_ID").as("PARENT_TARGET_ID"),
            parent_df.col("TARGET_ID").as("CHILD_TARGET_ID")
    );

// COMMAND ----------

//Get the Monitor type details into a dataframe 
var type_df = spark.read.option("header", "true").option("ignoreLeadingWhiteSpace",true).csv("s3://navisite-dataset-test/type.csv");
//Rename the columns in dataframe
type_df = type_df.select(
            type_df.col("TARGET_ID").as("MONITOR_ID"),
            type_df.col("TYPE").as("MONITOR_DESC")
    );

// COMMAND ----------

// Define the schema to be loaded into the event dataframe.
val schema = StructType(Array(StructField("destination", StringType, true), 
                                    StructField("core_time", StringType, true),
                                    StructField("poller_time", StringType, true),
                                    StructField("target_id", StringType, true), 
                                    StructField("policy_id", StringType, true),
                                    StructField("policy_type", StringType, true),
                                    StructField("detected_severity", StringType, true), 
                                    StructField("propagated_severity", StringType, true),
                                    StructField("low_threshold", StringType, true),
                                    StructField("measured_value", StringType, true), 
                                    StructField("high_threshold", StringType, true),
                                    StructField("propagated_flag", StringType, true),
                                    StructField("event_code", StringType, true), 
                                    StructField("metric_name", StringType, true)
                                    ))
//Get one week's event data into a dataframe 
var event_log_df = sqlContext.read.format("com.databricks.spark.csv").
                         option("mode", "DROPMALFORMED").option("ignoreLeadingWhiteSpace",true).
                         schema(schema).load("s3://navisite-dataset-test/event.log.20190909032250.csv.gz",
                         "s3://navisite-dataset-test/event.log.20190910032651.csv.gz",
                         "s3://navisite-dataset-test/event.log.20190911034326.csv.gz",
                         "s3://navisite-dataset-test/event.log.20190912034837.csv.gz",
                         "s3://navisite-dataset-test/event.log.20190913034449.csv.gz",
                         "s3://navisite-dataset-test/event.log.20190914032912.csv.gz",
                         "s3://navisite-dataset-test/event.log.20190915031653.csv.gz"
                         );

//Filter the event logs based on policy_type as "threshold" and metric name not equal to "Started"
event_log_df = event_log_df.filter("policy_type == 'T'" ).filter("metric_name != 'Started'");

// COMMAND ----------

/* 
  Get top customers from top_clients.csv.
  top_clients.csv is generated by taking the top 150 clients who own 65% of the devices.
*/
var top_clients_df = spark.read.option("header", "true").option("ignoreLeadingWhiteSpace",true).csv("s3://navisite-dataset-test/top_clients.csv");
//Rename the columns in dataframe
top_clients_df = top_clients_df.select(
            top_clients_df.col("NEW_BUS_ID").as("NEW_BUS_ID"),
            top_clients_df.col("RECONCILIATION_NUM").as("NUM_CUST_DEVICES"),
            top_clients_df.col("PERCENT").as("PERCENT"),
            top_clients_df.col("CUMULATIVE_PERCENT").as("CUMULATIVE_PERCENT"),
            top_clients_df.col("RANK").as("RANK")
    );

// COMMAND ----------

//Join event log dataframe with device dataframe
var device_event_log = event_log_df.join(device_df, event_log_df.col("TARGET_ID") === device_df.col("DEVICE_TARGET_ID"), "inner")
                            .drop("DEVICE_TARGET_ID");

// COMMAND ----------

//Join event log with customer data
var cust_device_event_log = device_event_log
    .join(customer_df, device_event_log.col("RECON_ID") === customer_df.col("RECONCILIATION_ID"), "inner").drop("RECONCILIATION_ID");

// COMMAND ----------

//Join event log with device details
var cust_detail_device_event_log = cust_device_event_log
    .join(bus_parent_df, cust_device_event_log.col("BUS_ID") === bus_parent_df.col("HIER_BUS_ID"), "left_outer").drop("HIER_BUS_ID");

// COMMAND ----------

//Filter event log to only keep identified top clients
var top_cust_detail_device_event_log = 
cust_detail_device_event_log.join(top_clients_df, cust_detail_device_event_log("BUS_ID") === top_clients_df("NEW_BUS_ID") || cust_detail_device_event_log("PARENT_BUS_ID") === top_clients_df("NEW_BUS_ID"), "inner").drop("CUMULATIVE_PERCENT", "RANK", "PERCENT"); 

// COMMAND ----------

//Joining the device details with the filtered event logs.
var top_cust_device_detail_event_log = top_cust_detail_device_event_log.join(device_location_df, top_cust_detail_device_event_log("RECON_ID") === device_location_df("RECONCILIATION_ID"), "left_outer").drop("BUS_ID_DEV").drop("RECONCILIATION_ID");

// COMMAND ----------

//Adding parent monitor details to the filtered event logs
var top_cust_device_monitor_event_log 
= top_cust_device_detail_event_log.join(parent_df, top_cust_device_detail_event_log("TARGET_ID") === parent_df("CHILD_TARGET_ID"), "left_outer").drop("CHILD_TARGET_ID");

// COMMAND ----------

//Adding monitor description to event logs
var event_logs_desc
= top_cust_device_monitor_event_log.join(type_df, top_cust_device_monitor_event_log("TARGET_ID") === type_df("MONITOR_ID"), "left_outer").drop("MONITOR_ID");


// COMMAND ----------

//Selecting required columns from the event logs
var event_logs_df = event_logs_desc.select(
    col("DESTINATION"),
    col("CORE_TIME"),
    to_timestamp(from_unixtime($"CORE_TIME")).as("CORE_TIMESTAMP"),
    col("POLLER_TIME"),
    to_timestamp(from_unixtime($"POLLER_TIME")).as("POLLER_TIMESTAMP"),
    col("TARGET_ID"),
    col("POLICY_ID"),
    col("DETECTED_SEVERITY"),
    col("PROPAGATED_SEVERITY"),
    col("LOW_THRESHOLD"),
    col("MEASURED_VALUE"),
    col("HIGH_THRESHOLD"),
    col("PROPAGATED_FLAG"),
    col("EVENT_CODE"),
    col("METRIC_NAME"),
    col("RECON_ID"),
    col("BUS_ID"),
    col("PARENT_BUS_ID"),
    col("NEW_BUS_ID"),
    col("NUM_CUST_DEVICES"),
    col("DEVICE_TYPE"),
    col("OPERATING_SYSTEM"),
    col("SITE"),
    col("SITE_DESC"),
    col("IS_VIRTUAL"),
    col("PARENT_TARGET_ID"),
    col("MONITOR_DESC")
    ); 

// COMMAND ----------

//Filtering out alarms from event logs
var events2 = event_logs_df.filter("DETECTED_SEVERITY != '0' OR PROPAGATED_SEVERITY != '0'").cache();
// Uncomment to check the count: events2.count();

// COMMAND ----------

//Get the incident details into a dataframe 
var incident_df = spark.read.option("header", "true").option("ignoreLeadingWhiteSpace",true).csv("s3://navisite-dataset-test/incident-utc-data/incidents.csv");

//Create a column setting value as 1 if incident source is monitor else 0.
incident_df = incident_df.withColumn(
      "Is_Source_Monitor",
      when(incident_df.col("submitter") === "Other", 0)
        .otherwise(1)
    );
//Rename columns    
incident_df = incident_df.select(
            incident_df.col("incident_id").as("INCIDENT_ID"),
            incident_df.col("submit_date").as("SUBMIT_DATE"),
            incident_df.col("bus_id").as("BUS_ID"),
            incident_df.col("event_code").as("EVENT_CODE"),
            incident_df.col("Is_Source_Monitor").as("Is_Source_Monitor")
    );

// COMMAND ----------

//Reading incident device details
var incident_device_df = spark.read.option("header", "true").option("ignoreLeadingWhiteSpace",true).csv("s3://navisite-dataset-test/incident-utc-data/incident-device.csv");
//Rename
incident_device_df = incident_device_df.select(
            incident_device_df.col("incident_id").as("INCIDENT_DEV_ID"),
            incident_device_df.col("recon_id").as("RECON_ID")
    );

// COMMAND ----------

//Adding device details to incident data
var incident_recon_df = incident_df.join(incident_device_df, incident_df("INCIDENT_ID") === incident_device_df("INCIDENT_DEV_ID"), "inner").drop("INCIDENT_DEV_ID");

// COMMAND ----------

//Rename
var incident_details_df = incident_recon_df.select(
            col("INCIDENT_ID"),
            col("SUBMIT_DATE").as("SUBMIT_DATE_MS"), 
            to_timestamp(from_unixtime($"SUBMIT_DATE")).as("SUBMIT_DATE"),
            col("BUS_ID"),
            col("EVENT_CODE").as("EVENT_CODE_INC"),
            col("RECON_ID").as("RECON_ID_INC"),
            col("Is_Source_Monitor")
            );

// COMMAND ----------

//Filtering incidents raised by monitor based on analysis start and end date
val FILTER_QUERY = "submit_date_ms >= " + INCIDENT_START_DATE + " AND submit_date_ms < " + INCIDENT_END_DATE;
var incidents2 = incident_details_df.filter("Is_Source_Monitor == '1'" ).filter(FILTER_QUERY).cache();

// COMMAND ----------

////////////////////////////////////////
// mapping incidents and events
////////////////////////////////////////

// COMMAND ----------

//Casting Core_time as an integer
val events3 = events2.withColumn("CORE_TIME_NUM", events2("CORE_TIME").cast(IntegerType))
    .drop("CORE_TIME");

// COMMAND ----------

//Casting incident submit date as an integer
val incidentsNum = incidents2.withColumn("SUBMIT_DATE_NUM", incidents2("SUBMIT_DATE_MS").cast(IntegerType))
    .drop("SUBMIT_DATE_MS");

// COMMAND ----------

//Mapping events and incidents based on device Id, event code and incident submit date
var mapping
= events3.join(incidentsNum, 
               events3("RECON_ID") === incidentsNum("RECON_ID_INC")
               && events3("EVENT_CODE") === incidentsNum("EVENT_CODE_INC")
               && incidentsNum("SUBMIT_DATE_NUM")-events3("CORE_TIME_NUM") <= EVENT_INCIDENT_INTERVAL
               && incidentsNum("SUBMIT_DATE_NUM")-events3("CORE_TIME_NUM") >= 0
               , "left_outer").drop("RECON_ID_INC").drop("EVENT_CODE_INC").drop("BUS_ID").drop("Is_Source_Monitor");
//mapping.count();

// COMMAND ----------

//Create another column for time difference in event and incident occurrence
var mapping2 = 
mapping.withColumn("INC_EVENT_TIME_DIFF", mapping("SUBMIT_DATE_NUM") - mapping("CORE_TIME_NUM"));

// COMMAND ----------

//Filtering out events which are not mapped to any incident to identify the mapping with minimum time difference.
var temp = mapping2.filter("SUBMIT_DATE_NUM != 0").groupBy("INCIDENT_ID", "RECON_ID").min("INC_EVENT_TIME_DIFF");

// COMMAND ----------

//Add a dummy column "HAS_MAPPED_INCIDENT" and mark all the minimum difference mapping as one
temp = temp.withColumn("HAS_MAPPED_INCIDENT", lit(1));
//Rename
temp = temp.select(
            col("INCIDENT_ID").as("INCIDENT_ID_TEMP"), 
            col("RECON_ID").as("RECON_ID_TEMP"), 
            col("min(INC_EVENT_TIME_DIFF)").as("MIN_DIFF"),
            col("HAS_MAPPED_INCIDENT")); 
//temp.show(100, false);

// COMMAND ----------

/*
  Final event incident mapping keeping the mapping with minimum time difference.
  HAS_MAPPED_INCIDENT will be 1 for all such incidents with minimum time difference.
*/  
var mapping4 = mapping2.join(temp, 
                             mapping2("INC_EVENT_TIME_DIFF") === temp("MIN_DIFF") && mapping2("RECON_ID") === temp("RECON_ID_TEMP") && mapping2("INCIDENT_ID") === temp("INCIDENT_ID_TEMP"), 
                             "left_outer").drop("RECON_ID_TEMP").drop("INCIDENT_ID_TEMP");

// COMMAND ----------

//Set JDBC properties for writing to the database
val prop = new java.util.Properties
prop.setProperty("driver", "org.mariadb.jdbc.Driver")
prop.setProperty("user", DB_USER)
prop.setProperty("password", DB_PASS) 

val url = "jdbc:mysql://" + DB_HOST + ":3306/" + DB_NAME + "?rewriteBatchedStatements=true"

// COMMAND ----------

//load the event incident mapping into database 
mapping4
	.write.mode("append")
    .option("createTableColumnTypes", "DESTINATION VARCHAR(10), CORE_TIMESTAMP timestamp, POLLER_TIME BIGINT, POLLER_TIMESTAMP timestamp, TARGET_ID VARCHAR(20), POLICY_ID VARCHAR(20), DETECTED_SEVERITY INT, PROPAGATED_SEVERITY INT, LOW_THRESHOLD VARCHAR(200), MEASURED_VALUE VARCHAR(200), HIGH_THRESHOLD VARCHAR(200), PROPAGATED_FLAG VARCHAR(20), EVENT_CODE VARCHAR(20), METRIC_NAME VARCHAR(100), RECON_ID VARCHAR(50), PARENT_BUS_ID VARCHAR(20), NEW_BUS_ID VARCHAR(20), NUM_CUST_DEVICES INT, DEVICE_TYPE VARCHAR(200), OPERATING_SYSTEM VARCHAR(200), SITE VARCHAR(200), SITE_DESC VARCHAR(200), IS_VIRTUAL VARCHAR(20), PARENT_TARGET_ID VARCHAR(20), MONITOR_DESC VARCHAR(200), CORE_TIME_NUM BIGINT, INCIDENT_ID VARCHAR(50), SUBMIT_DATE TIMESTAMP , SUBMIT_DATE_NUM BIGINT, INC_EVENT_TIME_DIFF INT,MIN_DIFF INT, HAS_MAPPED_INCIDENT INT").jdbc(url, EVENT_TABLE_NAME, prop);


// COMMAND ----------

//load the incidents into database
incidentsNum.write.mode("overwrite").option("createTableColumnTypes", "INCIDENT_ID VARCHAR(20), SUBMIT_DATE TIMESTAMP, BUS_ID VARCHAR(20), EVENT_CODE_INC VARCHAR(20), RECON_ID_INC VARCHAR(50), Is_Source_Monitor INTEGER, SUBMIT_DATE_NUM BIGINT").jdbc(url, INCIDENT_TABLE_NAME, prop)

