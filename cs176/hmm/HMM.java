import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.lang.StringBuilder;
import java.lang.Math;
import java.text.DecimalFormat;
import java.lang.ProcessBuilder;

public class HMM {

    private static final double[] STATES = {0.32, 1.75, 4.54, 9.40};
    private static final int NUMQSTATES = STATES.length;
    private static final int NUMSTATES = 2;
    private static final String DIRECTORY = "../ps5data/";

    private double[] marginal;
    private double[][] transition;
    private double[][] emission;
    private double[][] forward;
    private double[][] backward;
    private int[] observed;
    private double loglikelihood;
    private double[][] statePostProb;
    private double[][][] joinPostProb;

    public HMM(InputReader r, String inp) throws IOException {
	this.marginal = new double[NUMQSTATES];
	this.transition = new double[NUMQSTATES][NUMQSTATES];
	this.emission = new double[NUMQSTATES][NUMSTATES];
	for (int i = 0; i < NUMQSTATES; i++) {
	    r.nextToken();
	    marginal[i] = Math.log(r.nextDouble());
	}
	for (int i = 0; i < NUMQSTATES; i++)
	    for (int j = 0; j < NUMQSTATES; j++)
		transition[i][j] = Math.log(r.nextDouble());
	for (int i = 0; i < NUMQSTATES; i++) {
	    r.nextToken();
	    emission[i][0] = Math.log(r.nextDouble());
	    emission[i][1] = Math.log(r.nextDouble());
	}
	r.close();
	BufferedReader data = new BufferedReader(new FileReader(inp));
	StringBuilder seq1 = new StringBuilder();
	StringBuilder seq2 = new StringBuilder();
	String temp = new String();
	data.readLine();
	while (temp != null) {
	    if (temp.startsWith(">")) break;
	    seq1.append(temp);
	    temp = data.readLine();
	}
	temp = data.readLine();
	while (temp != null) {
	    seq2.append(temp);
	    temp = data.readLine();
	}
	data.close();
	String sequence1 = seq1.toString();
	String sequence2 = seq2.toString();
	this.observed = new int[seq1.length()];
	for (int i = 0; i < observed.length; i++)
	    this.observed[i] = (sequence1.charAt(i) == sequence2.charAt(i))
		? 0 : 1;
    }

    private double eexp(double x) {
	return (Double.isNaN(x)) ? 0 : Math.exp(x);
    }

    private double eln(double x) {
	return (x == 0) ? Double.NaN : Math.log(x);
    }

    private double elnsum(double x, double y) {
	if (Double.isNaN(x) || Double.isNaN(y))
	    return (Double.isNaN(x)) ? y : x;
	else return (x > y)
		 ? x + eln(1 + Math.exp(y - x))
		 : y + eln(1 + Math.exp(x - y));
    }

    private double elnproduct(double x, double y) {
	return (Double.isNaN(x) || Double.isNaN(y)) ? Double.NaN : x + y;
    }

    private void forward() {
	this.forward = new double[NUMQSTATES][observed.length];
	for (int i = 0; i < NUMQSTATES; i++)
	    forward[i][0] = elnproduct(marginal[i],
				       emission[i][observed[0]]);
	for (int t = 1; t < observed.length; t++) {
	    for (int j = 0; j < NUMQSTATES; j++) {
		double sum = Double.NaN;
		for (int i = 0; i < NUMQSTATES; i++)
		    sum = elnsum(sum, 
				 elnproduct(forward[i][t-1],
					    transition[i][j]));
		forward[j][t] = elnproduct(sum, emission[j][observed[t]]);
	    }
	}
	loglikelihood = Double.NaN;
	for (int i = 0; i < NUMQSTATES; i++)
	    loglikelihood = elnsum(loglikelihood,
				   forward[i][observed.length-1]);
    }

    private double[] viterbi() {
	double[][] vit = new double[NUMQSTATES][observed.length];
	int[][] ptr = new int[NUMQSTATES][observed.length];
	for (int i = 0; i < NUMQSTATES; i++) {
	    vit[i][0] = elnproduct(marginal[i],
				   emission[i][observed[0]]);
	}
	for (int t = 1; t < observed.length; t++) {
	    for (int j = 0; j < NUMQSTATES; j++) {
		double maxi = -Double.MAX_VALUE;
		int argmax = 0;
		for (int i = 0; i < NUMQSTATES; i++) {
		    double value = elnproduct(vit[i][t-1],
					      transition[i][j]);
		    if (value > maxi) {
			maxi = value;
			argmax = i;
		    }
		}
		ptr[j][t] = argmax;
		vit[j][t] = elnproduct(maxi, emission[j][observed[t]]);
	    }
	}
	int[] result = new int[observed.length];
	int argmax = 0;
	double maxi = -Double.MAX_VALUE;
	for (int i = 0; i < NUMQSTATES; i++) {
	    if (vit[i][observed.length-1] > maxi) {
		maxi = vit[i][observed.length-1];
		argmax = i;
	    }
	}
	result[observed.length-1] = argmax;
	for (int i = observed.length - 2; i >= 0; i--) {
	    result[i] = ptr[result[i+1]][i+1];
	}
	double[] ret = new double[observed.length];
	for (int i = 0; i < observed.length; i++)
	    ret[i] = STATES[result[i]];
	return ret;
    }

    private void backward() {
	this.backward = new double[NUMQSTATES][observed.length];
	for(int i = 0; i < NUMQSTATES; i++) {
	    backward[i][observed.length - 1] = 0;
	}
	for(int t = observed.length - 2; t > -1; t--) {
	    for(int k = 0; k < NUMQSTATES; k++) {
		double logbeta = Double.NaN;
		for(int j = 0; j < NUMQSTATES; j++) {
		    double value = elnproduct(emission[j][observed[t+1]],
					      backward[j][t+1]);
		    logbeta = elnsum(logbeta,
				     elnproduct(transition[k][j],
						value));
		}
		backward[k][t] = logbeta;
	    }
	}
    }
    
    private void computePostProb() {
	this.statePostProb = new double[NUMQSTATES][observed.length];
	for(int t = 0; t<observed.length; t++) {
	    double x = Double.NaN;
	    for(int i = 0; i<NUMQSTATES; i++) {
		statePostProb[i][t] = elnproduct(forward[i][t],
						 backward[i][t]);
		x = elnsum(x, statePostProb[i][t]);
	    }
	    for(int i = 0; i<NUMQSTATES; i++) {
		statePostProb[i][t] = elnproduct(statePostProb[i][t], -x);
	    }
	}
    }

    private double[][] posteriorDecoding() {
	double[][] ret = new double[2][observed.length];
	for (int t = 0; t < observed.length; t++) {
	    int argmax = 0;
	    double maxi = -Double.MAX_VALUE;
	    double total = 0;
	    for (int j = 0; j < NUMQSTATES; j++) {
		ret[1][t] += eexp(statePostProb[j][t]) * STATES[j];
		if (statePostProb[j][t] > maxi) {
		    maxi = statePostProb[j][t];
		    argmax = j;
		}
	    }
	    ret[0][t] = STATES[argmax];
	}
	return ret;
    }
    
    private void computeJoinPostProb() {
	this.joinPostProb
	    = new double[NUMQSTATES][NUMQSTATES][observed.length-1];
	for(int t = 0; t < observed.length - 1; t++) {
	    double x = Double.NaN;
	    for(int i = 0; i < NUMQSTATES; i++) {
		for(int j = 0; j < NUMQSTATES; j++) {
		    joinPostProb[i][j][t] = elnproduct(forward[i][t], elnproduct(transition[i][j], elnproduct(emission[j][observed[t+1]], backward[j][t+1])));
		    x = elnsum(x, joinPostProb[i][j][t]);
		}
	    }
	    for(int i = 0; i< NUMQSTATES; i++) {
		for(int j = 0; j < NUMQSTATES; j++) {
		    joinPostProb[i][j][t] = elnproduct(joinPostProb[i][j][t], -x);
		}
	    }
	}
    }
    
    private double[] getNewMarginalProb() {
	double[] newMarginal = new double[NUMQSTATES];
	for(int i = 0; i < NUMQSTATES; i++) {
	    newMarginal[i] = statePostProb[i][0];
	}
	return newMarginal;
    }
    
    private double[][] getNewTransitionProb() {
	double[][] newTransition = new double[NUMQSTATES][NUMQSTATES];
	for(int i = 0; i < NUMQSTATES; i++) {
	    for(int j = 0; j < NUMQSTATES; j++) {
		double num = Double.NaN;
		double den = Double.NaN;
		for(int t = 0; t < observed.length - 1; t++){
		    num = elnsum(num, joinPostProb[i][j][t]);
		    den = elnsum(den, statePostProb[i][t]);
		}
		newTransition[i][j] = elnproduct(num, -den);
	    }
	}
	return newTransition;
    }
    
    private double[][] getNewEmissionProb() {
	double[][] newEmission = new double[NUMQSTATES][NUMSTATES];
	for(int i = 0; i < NUMQSTATES; i++){
	    for(int x = 0; x < NUMSTATES; x++) {
		double num = Double.NaN;
		double den = Double.NaN;
		for(int t = 0; t < observed.length; t++) {
		    if(observed[t] == x) {
			num = elnsum(num, statePostProb[i][t]);
		    }
		    den = elnsum(den, statePostProb[i][t]);
		}
		newEmission[i][x] = elnproduct(num, -den);
	    }
	}
	return newEmission;
    }

    private void outputDecoding(boolean initial, String mu) {
	PrintWriter writer;
	try {
	    writer = new PrintWriter(DIRECTORY
				     + "decodings_"
				     + ((initial)
					? "initial"
					: "estimated")
				     + "_" + mu + ".txt", "UTF-8");
	    writer.println("# Viterbi_decoding"
			   + " posterior_decoding posterior_mean");
	    double[] vit = this.viterbi();
	    double[][] post = this.posteriorDecoding();
	    DecimalFormat df = new DecimalFormat("#.000000");
	    for (int i = 0; i < this.observed.length; i++) 
		writer.println(vit[i] + " "
			       + post[0][i] + " "
			       + df.format(post[1][i]));
	    writer.flush();
	    writer.close();
	}
	catch (Exception e) { System.out.println("FAILED TO OUTPUT"); }
    }

    private void outputLikelihood(double init, double post, String mu) {
	PrintWriter writer;
	try {
	    writer = new PrintWriter(DIRECTORY
				     + "likelihoods_"
				     + mu + ".txt", "UTF-8");
	    writer.println("# Likelihood under "
			   + "{initial, estimated} parameters");
	    DecimalFormat df = new DecimalFormat("#.000000");
	    writer.println(df.format(init));
	    writer.println(df.format(post));
	    writer.flush();
	    writer.close();
	}
	catch (Exception e) { System.out.println("FAILED TO OUTPUT"); }
    }

    private void outputParameters(String mu) {
	PrintWriter writer;
	try {
	    writer = new PrintWriter(DIRECTORY
				     + "estimated_parameters_"
				     + mu + ".txt", "UTF-8");
	    DecimalFormat df = new DecimalFormat("0.000000E00"); 
	    writer.println("# Marginal probabilities");
	    for (int i = 0; i < NUMQSTATES; i++)
		writer.println(df.format(eexp(marginal[i])));
	    writer.println("\n# Transition probabilities");
	    for (int i = 0; i < NUMQSTATES; i++) {
		for (int j = 0; j < NUMQSTATES; j++)
		    writer.print(df.format(eexp(transition[i][j])) + " ");
		writer.print("\n");
	    }
	    writer.println("\n# Emission probabilities");
	    for (int i = 0; i < NUMQSTATES; i++) {
		for (int j = 0; j < 2; j++)
		    writer.print(df.format(eexp(emission[i][j])) + " ");
		writer.print("\n");
	    }
	    writer.flush();
	    writer.close();
	}
	catch (Exception e) { System.out.println("FAILED TO OUTPUT"); }
    }

    private static class InputReader implements Closeable {
        private final BufferedReader in;
        private StringTokenizer tokenizer = new StringTokenizer("");
        public InputReader(String filename) throws FileNotFoundException {
            this.in = new BufferedReader(new FileReader(filename));
        }
	public void ignoreLine() throws IOException {
	    in.readLine();
	}
        public String nextToken() throws IOException {
	    String line;
            while (!tokenizer.hasMoreTokens()) {
		line = in.readLine();
		if (line == null) return null;
		while (line.startsWith("#")) line = in.readLine();
		tokenizer = new StringTokenizer(line);
            }
            return tokenizer.nextToken();
        }
        public int nextInt() throws IOException {
            return Integer.parseInt(nextToken());
        }
        public long nextLong() throws IOException {
            return Long.parseLong(nextToken());
        }
        public Double nextDouble() throws IOException {
            return Double.valueOf(nextToken());
        }
        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    public static void usage() {
	System.out.println("Usage: java HMM <initial> <sequence>");
	System.exit(0);
    }

    public void update() {
	forward();
	backward();
	computePostProb();
	computeJoinPostProb();
	marginal = getNewMarginalProb();
	transition = getNewTransitionProb();
	emission = getNewEmissionProb();
    }

    public static void main(String[] args) throws Exception {
	if (args.length < 2) usage();
	InputReader initial = new InputReader(DIRECTORY + args[0]);
	String sequencename = DIRECTORY + args[1];
	String stubname = (args[1].split("_")[1]).split("\\.")[0];

	HMM hmm = new HMM(initial, sequencename);
	hmm.update();
	double initiallikelihood = hmm.loglikelihood;
	System.out.println("Iteration 1: " + hmm.loglikelihood);
	hmm.outputDecoding(true, stubname);
	Process init = new ProcessBuilder("python",
					  "testbar.py",
					  "initial",
					  stubname).start();
	init.waitFor();
	System.out.println("Saved initial plot");
	for (int i = 2; i <= 15; i++) {
	    hmm.update();
	    System.out.println("Iteration " + i
			       + ": " + hmm.loglikelihood);
	}
	double postlikelihood = hmm.loglikelihood;
	hmm.outputDecoding(false, stubname);
	hmm.outputLikelihood(initiallikelihood, postlikelihood, stubname);
	hmm.outputParameters(stubname);
	Process estimate = new ProcessBuilder("python",
					      "testbar.py",
					      "estimated",
					      stubname).start();
	estimate.waitFor();
	System.out.println("Saved estimated plot");
    }
}
