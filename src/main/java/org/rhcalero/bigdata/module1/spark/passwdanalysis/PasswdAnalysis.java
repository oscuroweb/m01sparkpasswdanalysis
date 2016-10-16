package org.rhcalero.bigdata.module1.spark.passwdanalysis;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.rhcalero.bigdata.module1.spark.passwdanalysis.controller.PasswdAnalysisOperations;
import org.rhcalero.bigdata.module1.spark.passwdanalysis.model.PasswdLine;

import scala.Tuple2;

/**
 * 
 * PasswdAnalysis:
 * <p>
 * Java Spark programs that uses as input file /etc/passwd and do the follow operations
 * <ul>
 * <li>Count the number of users</li>
 * <li>Order the user by user name and take a subset</li>
 * <li>Count the number of users that having a specific command</li>
 * </ul>
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 16, 2016
 */
public class PasswdAnalysis {

    /** Log instance. */
    private static Logger log = Logger.getLogger(PasswdAnalysis.class.getName());

    /** Number of result for order operation. */
    private static int RESULTS_NUMBER = 5;

    /** Command for filter operation. */
    private static String COMMAND = "/bin/bash";

    /**
     * 
     * Method main.
     * <p>
     * Execute PasswdAnalysis programs
     * </p>
     * 
     * @param args List of arguments.
     */
    public static void main(String[] args) {

        // Validate arguments list. File name or directory is required
        if (args.length < 1) {
            log.fatal("[ERROR] RuntimeException: There must be at least one argument (a file name or directory)");
            throw new RuntimeException();
        }

        try {
            // Get initial time
            long initTime = System.currentTimeMillis();

            // Initialize Spark conf
            SparkConf conf = new SparkConf();

            // Initialize Spark Java context
            JavaSparkContext context = new JavaSparkContext(conf);

            // Initialize Passwd Analysis Operations.
            PasswdAnalysisOperations.setConf(conf);
            PasswdAnalysisOperations.setContext(context);

            // Initialize PassWd Pair from file
            PasswdAnalysisOperations.getPasswdPair(args[0]);

            // Invoke number of users operation
            Long numOfUsers = PasswdAnalysisOperations.getNumberOfUser();

            // Get user ordered by user name and taking the first RESULT_NUMBER values
            List<Tuple2<String, PasswdLine>> orderedUsers = PasswdAnalysisOperations.getSortedUsers(RESULTS_NUMBER);

            // Invoke number of user filtered by command operation
            Long numOfUsersFilterd = PasswdAnalysisOperations.getNumberOfUserFilteredByComand(COMMAND);

            // Stop context
            PasswdAnalysisOperations.stopContext();

            // Get computing time
            long computingTime = System.currentTimeMillis() - initTime;

            // Print operations result
            System.out.println("The number of user accounts is: " + numOfUsers);
            System.out.println("Users ordered by user name: ");
            for (Tuple2<String, PasswdLine> tuple : orderedUsers) {
                System.out.println(tuple._2().toString());
            }
            System.out.println("The number of users having bash as command when logging is: " + numOfUsersFilterd);

            System.out.println("Computing time: " + computingTime);

        } catch (Exception e) {
            log.fatal(e);
        }

    }
}
