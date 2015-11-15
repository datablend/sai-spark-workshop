package be.datablend.spark.lab2;

import be.datablend.spark.BasicSparkLab;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by dsuvee
 */
public class Exercise1 extends BasicSparkLab implements Serializable {

    @Test
    // Count the number of reviews in the file
    public void getReviewedBusinessesForEachReviewer() {
        // Count the number of reviews
        long count = 0;

        // Check whether it actually contains 335022 reviews
        Assert.assertEquals(335022, count);
    }

    @Test
    // Count the number of stars
    public void countNumberOfStars() {
        // Count the stars
        long count = 0;

        // Check whether it actually contains 335022 stars
        Assert.assertEquals(335022, count);
    }

    @Test
    // Count the number of reviews that gave 5 stars
    public void countNumberOfFiveStarReviews() {
        // Count the stars
        long count = 0;

        // Check whether it actually contains 119389 five star reviews
        Assert.assertEquals(119389, count);
    }

    @Test
    // Calculate the percentage of 3-star reviews on the full review data set
    public void calculatePercentualNumberOfThreeStarReviews() {
        // Calculate the percentage
        double percentage = 0.0;

        // Check whether it's the actual percentage
        Assert.assertEquals(0.14208022159738765, percentage, 0.0);
    }

    @Test
    // Count the number of business that have been granted 5 star reviews
    public void countNumberOfFiveStarBusinesses() {
        // Count the number of unique 5-star businesses
        long count = 0;

        // Check whether it actually contains 12698 businesses
        Assert.assertEquals(12698, count);
    }

    @Test
    // Cound the number of business that have received both 1-star and 5-star reviews (use the intersection transformation operator)
    public void countNumberOfCombinedOneStarAndFiveStarBusinesses() {
        // Count the number of businesses still included
        long count = 0;

        // Check whether it actually contains 7002 businesses
        Assert.assertEquals(7002, count);
    }

    @Test
    // Count the number of times the word "chinese" is mentioned in a review
    public void countNumberOfChineseWordMentions() {
        // Count the chinese word mentions
        long count = 0;

        // Check whether it actually contains 1469 chinese word mentions
        Assert.assertEquals(1469, count);

        // What if you would like to quickly execute this on just sample of the data?
    }

    @Test
    // Get a local list of all reviewers of a particular business with id sgBl3UDEcNYKwuUb92CYdA
    public void retrieveAllReviewersOfAParticularBusiness() {
        // Count our reviewers of interest
        long count = 0;

        // Check whether it actually contains 136 reviewers
        Assert.assertEquals(136, count);

        // What if we are only interested in the first 10?
    }

    @Test
    // Count the number of mentionings of the words coffee or tea in 5-star reviews (use the union transformation operator)
    public void countCoffeeOrTeaMentionsIn5StarReviews() {
        // Count the number of occurrences
        long count = 0;

        // Check whether it actually contains 11596 occurrences
        Assert.assertEquals(11596, count);
    }

}
