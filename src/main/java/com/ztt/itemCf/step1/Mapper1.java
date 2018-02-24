package com.ztt.itemCf.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author:zhangtietuo
 * @Description:
 * @Date: 2018/2/24 9:49
 */
public class Mapper1 extends Mapper<LongWritable, Text, Text, Text>{

    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * @date:2018/2/24 9:54
     * @author:zhangtietuo
     * @description:
     * @param key 行号
     * @param value A,1,1   C,5,5  等等
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        String userId = values[0];
        String itemId = values[1];
        String score = values[2];
        outKey.set(itemId);
        outValue.set(userId+"_"+score);
        context.write(outKey,outValue);



    }
}