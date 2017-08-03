package com.myhadoop.matrixdecomposition.lanczos;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.myhadoop.matrixdecomposition.datamodel.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AdjacentMatrixInputReducer
//extends Reducer<IntWritable,IntWritable,IntWritable,HashMapWritable>{
	extends Reducer<IntWritable,Text,IntWritable,HashMapWritable>{
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException,InterruptedException{
		List<Text> list = Lists.newArrayList(values);
		for (int i = 0; i < list.size(); i++) {
			HashMapWritable m = new HashMapWritable();
			for (int j = 0; j < list.size(); j++) {
				m.add(j, kmeanDistance(list.get(i), list.get(j)));
			}
			context.write(new IntWritable(i), m);
		}
	}


	public static double kmeanDistance(Text text1, Text text2) {
		String s1 = text1.toString();
		String s2 = text2.toString();

		double result = 0;
		String[] s1Array = s1.split(",");
		String[] s2Array = s2.split(",");
		for (int i = 0; i < s1Array.length; i++) {
			Double d1 = Double.valueOf(s1Array[i]);
			Double d2 = Double.valueOf(s2Array[i]);

			result += Math.pow((d1-d2), 2);
		}

		result = Math.sqrt(result);
		return result;
	}
	
}