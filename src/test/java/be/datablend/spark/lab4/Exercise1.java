package be.datablend.spark.lab4;

import be.datablend.spark.BasicSparkLab;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by dsuvee
 */
public class Exercise1 extends BasicSparkLab implements Serializable {

    @Test
    // Keep the parsing time as a side metric while processing the coffee and tea words
    public void parsingTimeWhileProcessingCoffeeOrTeaWords() {
        // Get the value of the distributed counter
        System.out.println("Processing time: " + 0);
    }

    @Test
    // Get the reviewed business with 5 stars but make use of broadcasting instead of joins to match up the business names
    public void getReviewedBusinessesWith5StarsWithNamesWithoutJoins() throws FileNotFoundException {
        // Find the value for the Postino Arcadia business
        List<Integer> businessCount = null;

        // Check whether it is effectively the Postino Acadia business
        Assert.assertEquals(473, (int)businessCount.get(0));
    }

    @Test
    // Get the mean number of stars for a particular business
    public void getMeanStarsForAParticularBusiness() {
        // Calculate the mean value
        double meanValue = 0.0;

        // Check whether it is 3.723404255319149
        Assert.assertEquals(3.723404255319149, meanValue, 0.0);
    }

}
