package com.hadoop.matrix.step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer2的目的是连接字符串(将结果连接在一起)
 */
public class Reducer2 extends Reducer<Text,Text,Text,Text>{
    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * key:行号
     * value:[列_值,列_值,列_值,列_值]
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for(Text value:values){
            sb.append(value.toString()+",");
        }
        String line = null;
        if(sb.toString().endsWith(",")){
            line = sb.substring(0,sb.length()-1);
        }
        outKey.set(key.toString());
        outValue.set(line);
        context.write(outKey,outValue);
    }
}
