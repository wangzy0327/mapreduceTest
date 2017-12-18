package com.hadoop.matrix.step1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MR1 {
    //输入文件相对路径
    //private static String inPath = "/user/wzy/matrix/step1_input/matrix2.txt";
    private static String inPath = "matrix/step1_input/matrix2.txt";
    //输出文件相对路径
    //private static String outPath = "/user/wzy/matrix/step1_output";
    private static String outPath = "matrix/step1_output";
    //hdfs文件地址
    private static String hdfs = "hdfs://localhost:9000";

    public int run() {
        try {
            //创建job作业的配置
            Configuration conf = new Configuration();
            //设置hdfs地址
            conf.set("fs.defaultFS", hdfs);
            //创建一个Job实例
            Job job = Job.getInstance(conf, "step1");
            //设置job的主类
            job.setJarByClass(MR1.class);

            //设置Map,Reduce的类型
            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reducer1.class);

            //设置Map输出key,value类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            //设置Reduce输出key,value类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileSystem fs = FileSystem.get(conf);
            //设置输入输出路径
            Path inputPath = new Path(inPath);
            if (fs.exists(inputPath)) {
                //将输入路径添加到job中
                FileInputFormat.addInputPath(job, inputPath);
            }
            Path outputPath = new Path(outPath);
            //如果路径存在则删除，否则不删除
            fs.delete(outputPath, true);
            //将输出路径添加到job中
            FileOutputFormat.setOutputPath(job, outputPath);

            return job.waitForCompletion(true) ? 1 : -1;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args) {
        int result = -1;
        result = new MR1().run();
        if (result == 1) {
            System.out.println("step1执行成功!");
        } else {
            System.out.println("step1执行失败!");
        }
    }
}
