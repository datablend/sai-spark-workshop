package be.datablend.spark.lab1;

import be.datablend.spark.BasicSparkLab;
import org.junit.Assert;
import org.junit.Test;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dsuvee
 */
public class Exercise2 extends BasicSparkLab implements Serializable {

    @Test
    // Create a distributed collection of sentences and filter the ones that do not contain spark
    public void filterSentencesAndCount() {
        // We create a list of strings (basically our sentences)
        List<String> input = new ArrayList<String>();
        input.add("spark is great");
        input.add("hadoop is great");
        input.add("spark beats hadoop");

        // Count the results
        long count = 0;

        // Check whether it actually contains 2 sentences
        Assert.assertEquals(2, count);
    }

}
