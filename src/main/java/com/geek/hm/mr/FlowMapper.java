package com.geek.hm.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split("\t");
		String phone = split[1];
		Integer upperFlow = Integer.valueOf(split[split.length-3]);
		Integer downFlow = Integer.valueOf(split[split.length-2]);
		FlowBean flowBean = new FlowBean();
		flowBean.setUpFlow(upperFlow);
		flowBean.setDownFlow(downFlow);
		flowBean.setCountFlow(upperFlow+downFlow);

		context.write(new Text(phone), flowBean);
	}
}
