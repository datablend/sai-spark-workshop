package be.datablend.spark.lab3;

import be.datablend.spark.BasicSparkLab;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dsuvee
 */
public class Exercise1 extends BasicSparkLab implements Serializable {

    @Test
    // Get the reviewed businesses for each reviewer
    public void getReviewedBusinessesForEachReviewer() {
        // Get the ones of reviewer JhnjcLJM5hiXK6R4minDsA
        List<String> businessIds = new ArrayList<String>();

        // Check whether user JhnjcLJM5hiXK6R4minDsA contains a review for business B8ujMtvvpHyEQ2r_QlAT2w
        Assert.assertTrue(businessIds.contains("B8ujMtvvpHyEQ2r_QlAT2w"));
    }

    @Test
    // Count the reviewed businesses for each reviewer
    public void countReviewedBusinessesForEachReviewer() {
        // Test it for JhnjcLJM5hiXK6R4minDsA
        long count = 0;

        // Check whether user JhnjcLJM5hiXK6R4minDsA actually has written 5 reviews
        Assert.assertEquals(5, count);
    }

    @Test
    // Count the number of coffee or tea mentions per business
    public void countNumberOfCoffeeAndTeaMentionsPerBusinesses() {
        // Group all words on the business id key
        Map<String, Object> countsPerBusiness = new HashMap<String, Object>();

        // Check whether the number of coffee or tea words mentions for business with id Oz1w_3Ck8lalmtxPcQMOIA is 55
        Assert.assertEquals(55L, countsPerBusiness.get("Oz1w_3Ck8lalmtxPcQMOIA"));
    }

    @Test
    // Get the businesses with the most 5-star reviews and find the number 1
    public void getTopReviewedBusinessesWith5Stars() {
        // Get the best one
        Tuple2<Integer, String> top = null;

        // Check whether it is effectively the business with id SDwYQ6eSu1htn8vHWv128g
        Assert.assertEquals("SDwYQ6eSu1htn8vHWv128g", top._2());
    }

    @Test
    // Calculate the average stars per business
    public void getAverageStarsPerBusiness() {
        // Get the average for business with id sgBl3UDEcNYKwuUb92CYdA
        double average = 0.0;

        // Check whether it is 3.723404255319149 for business sgBl3UDEcNYKwuUb92CYdA
        Assert.assertEquals(3.723404255319149, average, 0.0);
    }

    @Test
    // Get the real names of the business (instead of just the ids) for each 5-star reviewed business (use the join transformation)
    public void getReviewedBusinessesWith5StarsWithNames() {
        // Find the tuple for the SDwYQ6eSu1htn8vHWv128g business
        List<Tuple2<Integer, String>> businessTuple = null;

        // Check whether it is effectively the Postino Acadia business
        Assert.assertEquals("Postino Arcadia", businessTuple.get(0)._2());

        // What if we want to store the results?
    }

}
