package com.hadoop.matrix.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * key:1
     * value:1   1_0,2_3,3_-1,4_2,5_-3
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //注意制表符在txt中是8个空格，要在记事本里打开输入，不然空格数不一致,不识别\\t
        String[] rowAndLine = value.toString().split("\\t");
        //矩阵的行号
        String row = rowAndLine[0];
        System.out.println("矩阵行号:" + row);
        System.out.println(rowAndLine[1]);
        String[] line = rowAndLine[1].split(",");
        //1_0,2_3,3_-1,4_2,5_-3
        for (int i = 0; i < line.length; i++) {
            String column = line[i].split("_")[0];
            String valueStr = line[i].split("_")[1];
            //key:列号   value:行号_值
            outKey.set(column);
            outValue.set(row + "_" + valueStr);
            context.write(outKey, outValue);
        }
    }
}
