package com.test;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.Tag;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
public class ClusterCreation 
{
private static final Log LOG = LogFactory.getLog(ClusterCreation.class);
		
	//The method creates EMR Cluster and return cluster id
	
	public ClusterCreation()
	{
		
	}
	
	
	public String createEMRCluster()
	{
		
   String currentDateWithhms = currentDate("yyyyMMdd kk:mm:ss");	
	
	AmazonElasticMapReduceClient emr = (AmazonElasticMapReduceClient) AmazonElasticMapReduceClientBuilder.standard()
	.withRegion("eu-west-1")
	.build();
		
	// creation of Cluster
	 
	
	StepFactory stepFactory = new StepFactory(); 
	StepConfig enabledebugging = new StepConfig()
			.withName(System.getenv("StepConfig_with_name"))
			
			.withActionOnFailure(System.getenv("StepConfig_withFlow"))
			.withHadoopJarStep(stepFactory.newEnableDebuggingStep());

	HadoopJarStepConfig runExampleConfig = new HadoopJarStepConfig()
			.withJar(System.getenv("HadoopJarStepConfig_withJar"))
			.withMainClass(System.getenv("HadoopJarStepConfig_withMainClass"))
			.withArgs(System.getenv("HadoopJarStepConfig_withBucketName")); 
			//.withArgs("mcpt.sportal.commercial.bucket.trnfeepbnn/output");

	StepConfig customExampleStep = new StepConfig()
			.withName(System.getenv("StepConfig_hadoop_withName"))
			.withActionOnFailure(System.getenv("StepConfig_hadoop_withActionOnFailure"))
			.withHadoopJarStep(runExampleConfig);
	
	Application hadoop = new Application().withName(System.getenv("Application_configured"));
	RunJobFlowRequest request = new RunJobFlowRequest()
			
			.withName(System.getenv("RunJobFlowRequest_withname")+currentDateWithhms)
			.withReleaseLabel(System.getenv("RunJobFlowRequest_version"))
			.withSteps(enabledebugging, customExampleStep)
			.withApplications(hadoop)
			//added emr-log in mdev bucket
			.withLogUri(System.getenv("RunJobFlowRequest_withloguri"))
			.withServiceRole("EMR_DefaultRole")
			.withJobFlowRole("EMR_EC2_DefaultRole")
			.withInstances(new JobFlowInstancesConfig()
			.withEc2SubnetId(System.getenv("RunJobFlowRequest_withEc2SubnetId"))
			.withEc2KeyName(System.getenv("RunJobFlowRequest_withEc2KeyName"))
			.withAdditionalMasterSecurityGroups(System.getenv("RunJobFlowRequest_withAdditionalMasterSecurityGroups"))
			.withAdditionalSlaveSecurityGroups(System.getenv("RunJobFlowRequest_withAdditionalSlaveSecurityGroups"))
			.withInstanceCount(Integer.valueOf(System.getenv("RunJobFlowRequest_withInstanceCount")))
			.withKeepJobFlowAliveWhenNoSteps(false)    
			.withMasterInstanceType(System.getenv("RunJobFlowRequest_withMasterInstanceType"))
			.withSlaveInstanceType(System.getenv("RunJobFlowRequest_withSlaveInstanceType")));
	                 

	         request.setVisibleToAllUsers(true);
	               
	                
	                //EMR Tag
	                List<Tag> taglist = executeTag();
	                //System.out.println(taglist);
	                request.withTags(taglist);
	                //end of EMR Tag
	                
	                
	                
	                

	
	RunJobFlowResult result = emr.runJobFlow(request);
	LOG.info("This is result: " + result.getJobFlowId());
	
	/// End of creation
	
	
	return result.getJobFlowId();
	
	}
	
	private static String currentDate(String dateFormat)
	{
	Date currentdate = new Date();
	SimpleDateFormat format = new SimpleDateFormat(dateFormat);
	String DateToStr = format.format(currentdate);
	
	return DateToStr;
	}
	
	
		public List<Tag> executeTag()
		{
			List<Tag> taglist = new ArrayList<Tag>();
			
			Tag environmentTag = new Tag();
			environmentTag.setKey("enviornment");
			environmentTag.setValue("n");
			
			taglist.add(environmentTag);
			
			Tag ownerTag = new Tag();
			ownerTag.setKey("owner");
			ownerTag.setValue("commercial");
			
			taglist.add(ownerTag);
			
			Tag regionTag = new Tag();
			regionTag.setKey("region");
			regionTag.setValue("eu-west-1");
			
			taglist.add(regionTag);
			
			Tag regionidTag = new Tag();
			regionidTag.setKey("regionid");
			regionidTag.setValue("euw1");
			
			taglist.add(regionidTag);
			
			Tag sequencTag = new Tag();
			sequencTag.setKey("sequenceid");
			sequencTag.setValue("001");
			
			taglist.add(sequencTag);
			
			
			Tag builddateTage = new Tag();
			builddateTage.setKey("build_date");
			Date currentdate = new Date();
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			String DateToStr = format.format(currentdate);
			
			
            builddateTage.setValue(DateToStr);
            
            taglist.add(builddateTage);
            
            
			Tag application_serviceTag = new Tag();
			application_serviceTag.setKey("application_service");
			application_serviceTag.setValue("RMS_extract_CDR");
			
			taglist.add(application_serviceTag);
			
			Tag projectTag = new Tag();
			projectTag.setKey("project");
			projectTag.setValue("sportal");
			
			taglist.add(projectTag);
			
			Tag maintanceTag = new Tag();
			maintanceTag.setKey("mainatance_day");
			maintanceTag.setValue("sunday");
			
			taglist.add(maintanceTag);
			
			
			
			Tag mainatanceTimeTag = new Tag();
			mainatanceTimeTag.setKey("maintenance_time");
			mainatanceTimeTag.setValue("00:00:00");
			
			taglist.add(mainatanceTimeTag);
			
			
			Tag confidentialityTag = new Tag();
			confidentialityTag.setKey("confidentiality");
			confidentialityTag.setValue("highly confidential");
			
			taglist.add(confidentialityTag);
			
			Tag complianceTag = new Tag();
			complianceTag.setKey("complianceTag");
			complianceTag.setValue("pci");
			
			taglist.add(complianceTag);
			
			
			Tag applicationRoleTag = new Tag();
			applicationRoleTag.setKey("application_Role");
			applicationRoleTag.setValue("big_data_analysis");
			
			taglist.add(applicationRoleTag);
			
			Tag clusterTag = new Tag();
			clusterTag.setKey("cluster");
			clusterTag.setValue("n");
			
			taglist.add(clusterTag);
			
			Tag vpcseqTag = new Tag();
			vpcseqTag.setKey("vpcseq");
			vpcseqTag.setValue("001");
			
			taglist.add(vpcseqTag);
			
			
			Tag launchModeTag = new Tag();
			launchModeTag.setKey("launch_mode");
			launchModeTag.setValue("stepexec");
			
			taglist.add(launchModeTag);
			
			
			Tag releaseTag = new Tag();
			releaseTag.setKey("release_label");
			releaseTag.setValue("5.17.0");
			
			taglist.add(releaseTag);
			
			
			Tag applicationstTag = new Tag();
			applicationstTag.setKey("application");
			applicationstTag.setValue("hadoop");
			
			taglist.add(applicationstTag);
			
			Tag configurationsTag = new Tag();
			configurationsTag.setKey("configurations");
			configurationsTag.setValue("hadoop with auto scaling");
			
			taglist.add(configurationsTag);
			
			
			Tag keyTag = new Tag();
			keyTag.setKey("key");
			keyTag.setValue("sportal");
			
			taglist.add(keyTag);
			
			
			Tag subnetIdTag = new Tag();
			subnetIdTag.setKey("subnet_id");
			subnetIdTag.setValue("subnet-220cbe45");
			
			taglist.add(subnetIdTag);
			
			Tag logUriTag = new Tag();
			logUriTag.setKey("loguri");
			logUriTag.setValue(System.getenv("RunJobFlowRequest_withloguri"));
			
			taglist.add(logUriTag);
			
			Tag bootstrap_actionTag = new Tag();
			bootstrap_actionTag.setKey("bootstrap_action");
			bootstrap_actionTag.setValue("na");
			
			taglist.add(bootstrap_actionTag);
			
			return taglist;
			
			
			
		}
	
}




