package com.ztt.itemCf.step1;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author:zhangtietuo
 * @Description:
 * @Date: 2018/2/24 10:16
 */
public class Reducer1 extends Reducer<Text, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String itemId = key.toString();
        Map<String, Integer> map = new HashMap<String, Integer>();
        for(Text value : values) {
            String[] userAndScore = value.toString().split("_");
            String userId = userAndScore[0];
            String score = userAndScore[1];
            if(map.containsKey(userId)) {
                Integer preScore = Integer.valueOf(map.get(userId));
                map.put(userId, preScore + Integer.valueOf(score));
            } else {
                map.put(userId, Integer.valueOf(score));
            }
        }
        StringBuffer sb = new StringBuffer();
        for(Map.Entry<String, Integer> entry : map.entrySet()) {
            sb.append(entry.getKey() + "_" + entry.getValue() + ",");
        }
        if(sb.toString().endsWith(",")) {
            sb.substring(0,sb.length()-1);
        }

        outKey.set(itemId);
        outValue.set(sb.toString());
        context.write(outKey, outValue);

    }
}
