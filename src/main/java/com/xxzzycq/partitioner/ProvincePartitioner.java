package com.xxzzycq.partitioner;

import com.xxzzycq.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Created by yangchangqi on 2018/4/10.
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    public static HashMap<String, Integer> provinceDict = new HashMap<>();

    static {
        provinceDict.put("137", 0);
        provinceDict.put("133", 1);
        provinceDict.put("138", 2);
        provinceDict.put("135", 3);
    }

    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        String prefix = key.toString().substring(0, 3);
        Integer provinceId = provinceDict.get(prefix);
        return provinceId == null ? 4 : provinceId;
    }
}
