package Detection.Model;

import flanagan.analysis.Regression;
import flanagan.analysis.Stat;

import java.util.List;

/**
 * Created by Eddie on 2017/11/8.
 */
public class LinearModel {

    private double[] arrayX;
    private double[] arrayY;

    Regression regression;
    Stat stat;
    // all the variables for analysis: stdDev, stdErr, t
    Double pr = 0.95;
    Double stdDev = null;
    Double stdErr = null;
    Double t = null;
    double[] coefficients;
    Double correlateCoefficient = null;

    public LinearModel(Double[] arrayX, Double[] arrayY) {
        assert (arrayX.length == arrayY.length);
        int size = arrayX.length;
        this.arrayX = new double[size];
        this.arrayY = new double[size];
        for (int i = 0;i < size; i++) {
            this.arrayX[i] = arrayX[i];
            this.arrayY[i] = arrayY[i];
        }
    }

    public LinearModel(double[] arrayX, double[] arrayY) {
        assert (arrayX.length == arrayY.length);
        this.arrayX = arrayX.clone();
        this.arrayY = arrayY.clone();
    }

    public LinearModel(List<Double> arrayX, List<Double> arrayY) {
        assert (arrayX.size() == arrayY.size());
        int size = arrayX.size();
        this.arrayX = new double[size];
        this.arrayY = new double[size];
        for (int i = 0;i < size; i++) {
            this.arrayX[i] = arrayX.get(i);
            this.arrayY[i] = arrayY.get(i);
        }
    }

    /**
     * build analysis object and calculate all the parameters.
     */
    public void startAnalysis() {
        regression = new Regression(arrayX, arrayY);
        regression.linear();
        stat = new Stat(arrayY);
        stdDev = stat.standardDeviation_as_double();
        stdErr = stat.standardError_as_double();
        coefficients = regression.getBestEstimates().clone();
        t = Stat.studentstValue(pr, arrayX.length - 2);
    }

    /**
     * Given an <code>x</code>, calculate the confidence interval of the <code>y</code>
     *
     * @param givenX
     * @return
     */
    public Double[] getConfidenceInterval(Double givenX) {
        Double[] interval = new Double[2];
        if (stdErr == null || stdDev == null) {
            startAnalysis();
        }
        double YRoof = coefficients[0] + coefficients[1] * givenX;
        interval[0] = YRoof - t * stdErr;
        interval[1] = YRoof + t * stdErr;

        return interval;
    }

    public Double getCorrelationCoefficient() {
        if (correlateCoefficient == null) {
            regression.getXYcorrCoeff();
        }
        return correlateCoefficient;
    }

    /**
     * Set the pr for the t-distribution.
     * Default is 0.95
     *
     * @param pr
     */
    public void setPr(Double pr) {
        this.pr = pr;
    }


}
