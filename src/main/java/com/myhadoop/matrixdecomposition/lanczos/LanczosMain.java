package com.myhadoop.matrixdecomposition.lanczos;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;

import com.myhadoop.matrixdecomposition.datamodel.DistributedRowMatrix;

public class LanczosMain {

	/**
	 * @param args
	 * @throws URISyntaxException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, 
	         IOException, InterruptedException, URISyntaxException	{
		/*
		 * args:input directory,matrix size,desireRank
		 */
		System.out.println("start");
		long startall = System.currentTimeMillis();
		// TODO Auto-generated method stub
		String root = "hdfs://hadoop702.lt.163.org:8020/user/big";
		Path in = new Path("hdfs://hadoop702.lt.163.org:8020/user/big/DSIFTFeature");
		Path out = new Path("hdfs://hadoop702.lt.163.org:8020/user/big/perfoutput");
		Path tmp = new Path("hdfs://hadoop702.lt.163.org:8020/user/big/tmp");
		int size = 311814;
		int desireRank = 1000;
		long start_matread = System.currentTimeMillis();
		AdjacentMatrixInputJob.runJob(in, out);
		long end_matread = System.currentTimeMillis();
		
		DistributedRowMatrix mat = new DistributedRowMatrix(out,tmp,size);
		
		Path EigenVector = new Path("hdfs://hadoop702.lt.163.org:8020/user/big/eigenvector");
		LanczosStatus ls = new LanczosStatus(mat,desireRank);
		long start_lanczos = System.currentTimeMillis();
		HadoopLanczosSolver solver = new HadoopLanczosSolver();
		double[] ev = solver.solve(ls, EigenVector);
		long end_lanczos = System.currentTimeMillis();
		
		for (int i=0;i<ev.length;i++){
			System.out.println(ev[i]);
		}

		long endall = System.currentTimeMillis();
		System.out.println("finish");
		System.out.println("Totaltime: "+(endall-startall)/1000.0+" s");
		System.out.println("Matread time: "+(end_matread-start_matread)/1000.0+" s");
		System.out.println("Lanczos time: "+(end_lanczos-start_lanczos)/1000.0+" s");
	}

}
