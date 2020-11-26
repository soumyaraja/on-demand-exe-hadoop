# On-Demand EMR Creation in AWS
## Pre-requisite
* create lambda function
* create EMR roles and required access to lambda to spin up EMR cluster
* create trigger for lambda function
* create required lambda envrionment varibale(s)

## Steps
* lambda function to spin up EMR cluster return emr cluster id
* execute hadoop code (location to provided in lambda environment varibales)
* shut down EMR (if opt for auto termination)
* places files in aws s3 (hadoop job output)

