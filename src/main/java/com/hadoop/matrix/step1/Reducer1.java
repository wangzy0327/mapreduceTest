package com.hadoop.matrix.step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * key:列号
     * value:[行号_值,行号_值,行号_值...]
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String column = key.toString();
        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            //text:行号_值
            sb.append(value.toString() + ",");
        }
        String line = null;
        if (sb.toString().endsWith(",")) {
            line = sb.substring(0, sb.length() - 1);
        }
        outKey.set(column);
        outValue.set(line);
        context.write(outKey, outValue);
    }
}
