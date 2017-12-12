package com.hadoop.matrix.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Mapper2的目的是计算出矩阵每个位置的值
 */
public class Mapper2 extends Mapper<LongWritable,Text,Text,Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    //获取缓存中每一行数据
    private List<String> cacheList = new ArrayList<String>();

    /**
     * setup在map执行前最开始执行，且只执行一次
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //通过输入流将全局缓存中的右侧矩阵读入List<String>中
        FileReader fr = new FileReader("matrix2");
        BufferedReader br = new BufferedReader(fr);

        //每一行的格式是: 行 tab 列_值,列_值,列_值,列_值
        String line = null;
        while((line = br.readLine())!=null){
            cacheList.add(line);
        }
        fr.close();
        br.close();
    }

    /**
     *  key:行号
     * value:行号	列_值,列_值,列_值,列_值
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //行
        String row_matrix1 = value.toString().split("\\t")[0];
        //列
        String[] column_value_array_matrix1 = value.toString().split("\\t")[1].split(",");
        for(String line:cacheList){
            //右侧矩阵的行
            //格式:行 tab 列_值,列_值,列_值,列_值,列_值
            String row_matrix2 = line.split("\\t")[0];
            //右侧矩阵的列
            String[] column_value_array_matrix2 = line.split("\\t")[1].split(",");
            //矩阵两行相乘的结果
            int result = 0;
            //遍历左矩阵每一行的每一列
            for(String column_value_matrix1:column_value_array_matrix1){
                //左矩阵列号
                String column_matrix1 = column_value_matrix1.split("_")[0];
                //左矩阵列值
                String value_matrix1 = column_value_matrix1.split("_")[1];
                //遍历右矩阵的每一行每一列
                for(String column_value_matrix2:column_value_array_matrix2){
                    if(column_matrix1 == column_value_matrix2.split("_")[0]){
                        String value_matrix2 = column_value_matrix2.split("_")[1];
                        result += Integer.valueOf(value_matrix1)*Integer.valueOf(value_matrix2);
                    }
                }
            }
            //result是结果矩阵中的某元素，坐标为   行：row_matrix1 , 列:row_matrix2(因为右矩阵已经转置)
            outKey.set(row_matrix1);
            outValue.set(row_matrix2+"_"+result);
            //输出格式 key:行  value:列_值
            context.write(outKey,outValue);
        }
    }
}
