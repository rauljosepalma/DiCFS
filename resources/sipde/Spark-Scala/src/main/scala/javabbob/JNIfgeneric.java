package javabbob;

import java.util.Random;
import java.io.File;

/**
 * JNI connection class for interfacing the needed BBOB C-functions in fgeneric.
 * @author mike, raymond
 * 
 * @contributor Diego Teijeiro <diego.teijeiro@udc.es>
 * @modified 2015-09-23
 * 
 *  Added two minor changes to the original JNI interface:
 *  
 *  	1. The JNIfgeneric and Params classes now implement java.io.Serializable
 *  	2. Added a new checkBBOB method to the class
 *  
 */
public class JNIfgeneric implements java.io.Serializable{

    /** Load the library, the system-specific filename extension is added by the JVM. */
    static {
        System.loadLibrary("cjavabbob");
    }

    /**
     * Class of the optional parameters to fgeneric.
     */
    static public class Params implements java.io.Serializable{
        /** Algorithm name.
         *  Default is "not-specified". */
        public String algName = "not-specified";
        /** Comment line.
         *  Default is "". */
        public String comments = "";
        /** Index and data file names prefix.
         *  Default is "bbobexp". */
        public String filePrefix = "bbobexp";
    }
    // Need to make sure the default values are the same as the ones in C
    // Is it possible to collect the default values in C?

    /** Deletes all files and subdirectories under dir.
     *  (method obtained from the Java developers almanac)
     *  Will remove the File instance dir (which should be a directory) and
     *  all files and subdirectories under dir. Will do nothing if dir is
     *  not a directory.
     *  @param dir an instance of File which should be a directory.
     *  @return true if all deletions were successful.
     *  If a deletion fails, the method stops attempting
     *  to delete and returns false.  */
    protected static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }

        // The directory is now empty so delete it
        return dir.delete();
    }

    /** Creates required directories which will contain the data.
     *  This method creates the directories data_f1 to data_f24, and data_f101
     *  to data_f130 for the noisy functions in the folder designated by the
     *  String outputPath.
     *  @param outputPath folder name in which the directories will be created
     *  @param overwrite if true, any former directories are deleted and
     *  recreated, if false and a directory exists at outputPath, nothing
     *  is changed and false returned
     *  @return true if the directories could be created or if they
     *  existed already, false otherwise  */
    public static boolean makeBBOBdirs(String outputPath, boolean overwrite) {

        boolean ret = true;

        File output = new File(outputPath);
        File newdir = null;

        if (output.isDirectory()) {
            if (overwrite) {
                ret = deleteDir( output );
            }
        }

        if( ret ) {
            // no noise directories
            for( int i=1; i<=24; i++ ) {
                newdir = new File(output, "data_f"+String.valueOf(i));
                if (! newdir.isDirectory()) {
                    ret = newdir.mkdirs();
                }
                if ( !ret ) return ret;
            }
            // now make noise directories
            for( int i=101; i<=130; i++ ) {
                newdir = new File(output, "data_f"+String.valueOf(i));
                if (! newdir.isDirectory()) {
                    ret = newdir.mkdirs();
                }
                if ( !ret ) return ret;
            }
        }

        return ret;
    }

    //Native method declaration

    /** Initializes the BBOB C backend.
     *   Has to be called each time any of the
     *   parameters (e.g. dimensions, functionID) changes; do not forget to
     *   call exitBBOB() before again calling initialize.
     *   @return the target function value of the specified fitness function */
    public native double initBBOB(int funcId, int instanceId, int dim,
                                  String datapath, Params optParams);

    /** Closes down the BBOB C backend initiated by initialize.
     *  @return the best true fitness value ever obtained */
    public native double exitBBOB();

    /** Determines if the benchmark is initialized (from the library side).
     *  @return true if it is, false otherwise
     * */
    public native boolean checkBBOB();
    
    /** Determines if the function given by funcId is part of the testbed.
     *  @return true if it is, false otherwise
     * */
    public native boolean exist(int funcId);
    // exist() function is missing

    /** Returns the target objective function value.
     *  @return the target function value.*/
    public native double getFtarget();
    // Should be renamed getFtarget

    /** Returns the best function value obtained so far.
     *  If any search points have been evaluated already (see evaluate()),
     *  the current best value is returned, otherwise it is the value DBL_MAX
     *  in C. Is the same as getFbest().
     *  @return the best function value obtained so far.  */
    public native double getBest();

    /** Returns the best function value obtained so far.
     *  If any search points have been evaluated already (see evaluate()),
     *  the current best value is returned, otherwise it is the value DBL_MAX
     *  in C. Is the same as getBest().
     *  @return the best function value obtained so far.  */
    public native double getFbest();


    /** Returns the number of function since initialize() was called.
     *  @return the number of function evaluations since initialize() was called. */
    public native double getEvaluations();

    /** Sets the noise random seed for the noisy functions.
     *  This function is provided for comparing the values of noisy functions
     *  for different implementation of fgeneric. */
    public native void setNoiseSeed(int seed);

    /** Provides with a uniform random value generator.
     *  This function is provided for comparing the values of functions
     *  for different implementation of fgeneric. It fills the input argument r
     *  with double values comprised between 0 and 1.
     *  @param r        a double array to be filled (size must be larger than N)
     *  @param N        an integer giving the number of random doubles to put in r
     *  @param inseed   an integer seed
     */
    public native void unif(double[] r, int N, int inseed);

    /** Returns the fitness value of X.
     *  X is a double array representing the decision variable vector.
     *  The search interval for X is [-5, 5] in each component. evalute(X)
     *  will return a reasonable value nonetheless for any real-valued X.
     *  Occasionally data are written in the data files.
     *  @param X decision variable vector
     *  @return the objective function value for the given search point */
    public native double evaluate(double[] X);

    /* Main method

    /**
     * Test Method.
     * @param args the command line arguments

    public static void main(String[] args) {
        // TODO code application logic here

        int dim = 10;
        double[] X = new double[dim];
        Random rand = new Random();

        JNIfgeneric fgeneric = new JNIfgeneric();
        JNIfgeneric.Params params = new JNIfgeneric.Params();

        double ret = fgeneric.initBBOB(2, 1, dim, "../data", params);
        if (ret == 0.) {
            System.out.println("initBBOB finished OK.");
        } else {
            System.out.println("initBBOB returned error: " + ret);
        }

        System.out.println("target=" + fgeneric.getFtarget());
        System.out.println("doing some test evalations:");
        for (int t = 0; t < 10; t++) {
            for (int i = 0; i < X.length; i++) {
                X[i] = rand.nextDouble();
            }
            System.out.print(fgeneric.evaluate(X) + " ");
        }

        System.out.println("\nbest=" + fgeneric.getBest());
        System.out.println("evaluations=" + fgeneric.getEvaluations());
        fgeneric.setNoiseSeed(1);

        ret = fgeneric.exitBBOB();
        if (ret == 0.) {
            System.out.println("exitBBOB finished OK.");
        } else {
            System.out.println("exitBBOB returned error: " + ret);
        }
    }*/
}
