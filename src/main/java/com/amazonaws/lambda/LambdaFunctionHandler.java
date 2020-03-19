package com.amazonaws.lambda;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.test.ClusterCreation;
import com.test.S3LowLevel;



public class LambdaFunctionHandler implements RequestHandler<SNSEvent, String> {
	private static final Log LOG = LogFactory.getLog(LambdaFunctionHandler.class);	
	
	
	private static String bucketName = System.getenv("HadoopJarStepConfig_withBucketName");
	private static String key = "clusterId/";
	
	@Override
    public String handleRequest(SNSEvent event, Context context) {
        context.getLogger().log("Received event: " + event);
        String message = event.getRecords().get(0).getSNS().getMessage();
        LOG.info(message);
        context.getLogger().log("From SNS: " + message);
        
        
        
        
    	
             ClusterCreation client = new ClusterCreation();
    		
    		//System.out.println(Region.getRegion(Regions.EU_WEST_1).getName());
    		String clusterId = client.createEMRCluster();
    		
    		//put jobflowid in s3
    		S3LowLevel s3client = new S3LowLevel();
    		try {
				s3client.putJobid(bucketName,key,clusterId);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    		//publishMsg(clusterId);
    		
    		return message;
    }
	
	}
