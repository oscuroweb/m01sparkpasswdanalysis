package org.rhcalero.bigdata.module1.spark.passwdanalysis.model;

import java.io.Serializable;

import org.apache.log4j.Logger;

/**
 * PasswdLine:
 * <p>
 * POJO class that representa a passwd line
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 16, 2016
 */
public class PasswdLine implements Serializable {

    /** Generated serial version */
    private static final long serialVersionUID = -4483811771269839994L;

    /** Log instance. */
    private static Logger log = Logger.getLogger(PasswdLine.class.getName());

    /** User name (a string). */
    private String username;
    /** A character (* or x, depending on the system). */
    private String hiddePasswd;
    /** The user identifier or UID (an integer value). */
    private Integer uid;
    /** The group identifier or GID (an integer value). */
    private Integer gid;
    /** The full user name (a string). */
    private String fullUsername;
    /** The home directory of the user (a string). */
    private String home;
    /** The command to run when the user logs in the system (a string). */
    private String command;

    /** Default separator. */
    private static final String SEPARATOR = ":";
    /** Number of fields. */
    private static final int FIELDS_NUMBER = 7;

    /**
     * 
     * Constructor for PasswdLine.
     * <p>
     * Construct a PasswdLine object from a String line using SEPARATOR
     * </p>
     * 
     * @param line File line
     */
    public PasswdLine(String line) {

        if (line != null) {
            // Split the line using SEPARATOR character and set to a array
            String[] lineArray = line.split(SEPARATOR);

            if (lineArray.length == FIELDS_NUMBER) {
                // Informs passwdLine properties.
                this.setUsername(lineArray[0]);
                this.setHiddePasswd(lineArray[1]);
                if (lineArray[2] != null && !lineArray[2].isEmpty()) {
                    this.setUid(Integer.valueOf(lineArray[2]));
                }
                if (lineArray[3] != null && !lineArray[3].isEmpty()) {
                    this.setGid(Integer.valueOf(lineArray[3]));
                }
                this.setFullUsername(lineArray[4]);
                this.setHome(lineArray[5]);
                this.setCommand(lineArray[6]);
            } else {
                String errorMessage = "[ERROR] Incorrect passwd line number of fields for line " + line;
                log.fatal(errorMessage);
                throw new RuntimeException(errorMessage);
            }
        }
    }

    /**
     * Method getUsername.
     * <p>
     * Get method for attribute username
     * </p>
     * 
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * Method setUsername.
     * <p>
     * Set method for attribute username
     * </p>
     * 
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Method getHiddePasswd.
     * <p>
     * Get method for attribute hiddePasswd
     * </p>
     * 
     * @return the hiddePasswd
     */
    public String getHiddePasswd() {
        return hiddePasswd;
    }

    /**
     * Method setHiddePasswd.
     * <p>
     * Set method for attribute hiddePasswd
     * </p>
     * 
     * @param hiddePasswd the hiddePasswd to set
     */
    public void setHiddePasswd(String hiddePasswd) {
        this.hiddePasswd = hiddePasswd;
    }

    /**
     * Method getUid.
     * <p>
     * Get method for attribute uid
     * </p>
     * 
     * @return the uid
     */
    public Integer getUid() {
        return uid;
    }

    /**
     * Method setUid.
     * <p>
     * Set method for attribute uid
     * </p>
     * 
     * @param uid the uid to set
     */
    public void setUid(Integer uid) {
        this.uid = uid;
    }

    /**
     * Method getGid.
     * <p>
     * Get method for attribute gid
     * </p>
     * 
     * @return the gid
     */
    public Integer getGid() {
        return gid;
    }

    /**
     * Method setGid.
     * <p>
     * Set method for attribute gid
     * </p>
     * 
     * @param gid the gid to set
     */
    public void setGid(Integer gid) {
        this.gid = gid;
    }

    /**
     * Method getFullUsername.
     * <p>
     * Get method for attribute fullUsername
     * </p>
     * 
     * @return the fullUsername
     */
    public String getFullUsername() {
        return fullUsername;
    }

    /**
     * Method setFullUsername.
     * <p>
     * Set method for attribute fullUsername
     * </p>
     * 
     * @param fullUsername the fullUsername to set
     */
    public void setFullUsername(String fullUsername) {
        this.fullUsername = fullUsername;
    }

    /**
     * Method getHome.
     * <p>
     * Get method for attribute home
     * </p>
     * 
     * @return the home
     */
    public String getHome() {
        return home;
    }

    /**
     * Method setHome.
     * <p>
     * Set method for attribute home
     * </p>
     * 
     * @param home the home to set
     */
    public void setHome(String home) {
        this.home = home;
    }

    /**
     * Method getCommand.
     * <p>
     * Get method for attribute command
     * </p>
     * 
     * @return the command
     */
    public String getCommand() {
        return command;
    }

    /**
     * Method setCommand.
     * <p>
     * Set method for attribute command
     * </p>
     * 
     * @param command the command to set
     */
    public void setCommand(String command) {
        this.command = command;
    }

    /**
     * Method toString.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(username).append(":").append(uid).append(":").append(gid);
        return builder.toString();
    }

}
