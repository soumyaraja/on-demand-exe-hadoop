package com.test;

import java.util.ListIterator;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;

public class EMRUtil 
{
   public EMRUtil()
   {
	   
   }
   
   public static boolean clusterExists(AmazonElasticMapReduce client, String clusterIdentifier) {
		if (clusterIdentifier != null && !clusterIdentifier.isEmpty()) {
			ListClustersResult clustersList = client.listClusters();
			ListIterator<ClusterSummary> iterator = clustersList.getClusters().listIterator();
			ClusterSummary summary;
			for (summary = iterator.next() ; iterator.hasNext();summary = iterator.next()) {
				if (summary.getId().equals(clusterIdentifier)) {
					DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest().withClusterId(clusterIdentifier);	
					DescribeClusterResult result = client.describeCluster(describeClusterRequest);	
					if (result != null) {
						Cluster cluster = result.getCluster();
						
						
						String state = cluster.getStatus().getState();
						System.out.println(clusterIdentifier + " is " + state + ". ");
						if (state.equalsIgnoreCase("STARTING ") ||state.equalsIgnoreCase("BOOTSTRAPPING")||state.equalsIgnoreCase("RUNNING"))	{
							System.out.println("The cluster with id " + clusterIdentifier + " exists and is " + state);   
							return true;
						}
					}
				}		
			}					
		}
		System.out.println("The cluster with id " + clusterIdentifier + " does not exist");
		return false;  
	}
}
