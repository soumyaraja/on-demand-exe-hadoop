package com.test;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3LowLevel {
private static final Log LOG = LogFactory.getLog(S3LowLevel.class);	
	public S3LowLevel()
	{
		
	}

	public String  putJobid(String bucketName, String key, String objectName) throws IOException {
		String clientRegion = "eu-west-1";

		

		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				          .withRegion(clientRegion)
				          .build();
		String id = null;

		// true to create object

		//if (s3Client.listObjects(bucketName, key).getObjectSummaries().isEmpty()) 
		//{
			//System.out.println("if Block");
			s3Client.putObject(bucketName, key + objectName, objectName);
			id = "done";
		//}

		// false to delete object
		/*else
		{
			
			System.out.println("Else Block");
			ListObjectsV2Request req = new  ListObjectsV2Request().withBucketName(bucketName).withPrefix(key);
			ListObjectsV2Result listing = s3Client.listObjectsV2(req);
			for(S3ObjectSummary summary: listing.getObjectSummaries()) 
			{

				System.out.println(summary.getKey()); 
				 id =summary.getKey().replace(key, ""); System.out.println(id);
				s3Client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(summary.getKey()));
			}
		}*/
          return id;
	}
}
