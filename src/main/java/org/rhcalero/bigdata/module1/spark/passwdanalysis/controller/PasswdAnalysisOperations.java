package org.rhcalero.bigdata.module1.spark.passwdanalysis.controller;

import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rhcalero.bigdata.module1.spark.passwdanalysis.model.PasswdLine;

import scala.Tuple2;

/**
 * PasswdAnalysisOperations:
 * <p>
 * Implements passwd operations
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 16, 2016
 */
public abstract class PasswdAnalysisOperations implements Serializable {

    /** Serial version */
    private static final long serialVersionUID = -4373688517043945517L;

    /** Log instance. */
    private static Logger log = Logger.getLogger(PasswdAnalysisOperations.class.getName());

    /** SparkConf object. */
    private static SparkConf conf = null;

    /** Java Spark Context. */
    private static JavaSparkContext context = null;

    /** RDD pair where key is the user name and value is a instance of PasswdLine */
    private static JavaPairRDD<String, PasswdLine> passwdPair = null;

    /**
     * Method setConf.
     * <p>
     * Set method for attribute conf
     * </p>
     * 
     * @param conf the conf to set
     */
    public static void setConf(SparkConf conf) {
        PasswdAnalysisOperations.conf = conf;
    }

    /**
     * Method setContext.
     * <p>
     * Set method for attribute context
     * </p>
     * 
     * @param context the context to set
     */
    public static void setContext(JavaSparkContext context) {
        PasswdAnalysisOperations.context = context;
    }

    /**
     * 
     * Method getPasswdPair.
     * <p>
     * Inform passwdPair using input file name
     * </p>
     * 
     * @param file Input file name.
     */
    public static void getPasswdPair(String file) {

        // Get Java RDD lines
        JavaRDD<String> lines = context.textFile(file);

        // Inform passwdPair from lines map and store in cache
        passwdPair = lines.mapToPair(line -> getPairFromLine(line)).cache();

    }

    /**
     * 
     * Method getNumberOfUser.
     * <p>
     * Obtain the numbers of user
     * </p>
     * 
     * @return Number of users
     * @throws Exception Generic exception
     */
    public static Long getNumberOfUser() throws Exception {

        Long userCount = null;

        if (passwdPair != null) {
            // Get count from passwdPair.
            userCount = passwdPair.count();
        } else {
            String errorMessage = "[ERROR] passwdPair object not initialized. Invoke method PasswdAnalysisOperations.getPassPair(String)";
            log.fatal(errorMessage);
            throw new Exception(errorMessage);
        }

        return userCount;
    }

    /**
     * 
     * Method getSortedUsers.
     * <p>
     * Order passwdPair instance by user name and limit the output to numOfResult
     * </p>
     * 
     * @param numOfResult Number of result to return
     * @return RDD Pair with sorted users
     * @throws Exception Generic exception
     */
    public static List<Tuple2<String, PasswdLine>> getSortedUsers(int numOfResult) throws Exception {

        List<Tuple2<String, PasswdLine>> sortedTuple = null;

        if (passwdPair != null) {
            // Order passwdPair by user name and take the first numOfResult
            sortedTuple = passwdPair.sortByKey().take(numOfResult);
        } else {
            String errorMessage = "[ERROR] passwdPair object not initialized. Invoke method PasswdAnalysisOperations.getPassPair(String)";
            log.fatal(errorMessage);
            throw new Exception(errorMessage);
        }

        return sortedTuple;

    }

    /**
     * 
     * Method getNumberOfUserFilteredByComand.
     * <p>
     * Filter passwdPair by command and return the number of results
     * </p>
     * 
     * @param command Command uses to filter
     * @return Number of results
     * @throws Exception Generic exception
     */
    public static Long getNumberOfUserFilteredByComand(String command) throws Exception {

        Long userCount = null;

        if (passwdPair != null) {
            // Filter passwdPair by command
            userCount = passwdPair.filter(passwdTuple -> equalCommand(passwdTuple, command)).count();
        } else {
            String errorMessage = "[ERROR] passwdPair object not initialized. Invoke method PasswdAnalysisOperations.getPassPair(String)";
            log.fatal(errorMessage);
            throw new Exception(errorMessage);
        }

        return userCount;

    }

    /**
     * 
     * Method stopContext.
     * <p>
     * Stop Spark context.
     * </p>
     */
    public static void stopContext() {
        if (context != null) {
            context.stop();
            context.close();
        }
    }

    /**
     * 
     * Method getPairFromLine.
     * <p>
     * Transform line info to a Tuple2
     * </p>
     * 
     * @param line File line
     * @return A Tuple2 where key is the username and passwd is a instance of PasswdLine
     */
    private static Tuple2<String, PasswdLine> getPairFromLine(String line) {

        // Construct passwdLine
        PasswdLine passwdLine = new PasswdLine(line);

        return new Tuple2<String, PasswdLine>(passwdLine.getUsername(), passwdLine);

    }

    /**
     * 
     * Method equalCommand.
     * <p>
     * Obtain if passwdLine command is the same that input parameter command.
     * </p>
     * 
     * @param tuple passwdLine tuple
     * @param command Command use to filter
     * @return true if commands are equals
     */
    private static boolean equalCommand(Tuple2<String, PasswdLine> tuple, String command) {
        boolean isEqual = false;

        if (tuple != null && tuple._2() != null && tuple._2().getCommand() != null) {
            isEqual = tuple._2().getCommand().equals(command);
        }

        return isEqual;
    }

}
