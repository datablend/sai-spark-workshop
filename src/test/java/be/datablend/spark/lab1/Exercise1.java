package be.datablend.spark.lab1;

import be.datablend.spark.BasicSparkLab;
import org.junit.Assert;
import org.junit.Test;
import java.io.Serializable;

/**
 * Created by dsuvee
 */
public class Exercise1 extends BasicSparkLab implements Serializable {

    @Test
    // Create a distributed collection of integers (1->1000000) and check that it effectively contains the required number of elements
    public void countNumberOfIntegers() {
        // Count the number of elements
        long count = 0;

        // Check whether it actually contains a million elements
        Assert.assertEquals(1000000, count);
    }

    @Test
    // Create a distributed collection of integers (1->1000000) and sum them all
    public void sumNumberOfIntegers() {
        // Sum the contained elements
        long sum = 0;

        // Check whether the sum equals 1783293664
        Assert.assertEquals(1783293664, sum);
    }

}
