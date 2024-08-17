import boto3

if __name__ == '__main__':
    # Initialize the boto3 clients
    cf_client = boto3.client('cloudformation')
    batch_client = boto3.client('batch')

    # The name of the CloudFormation stack
    stack_name = 'test-batch'  # Replace with your CloudFormation stack name

    # Get the stack outputs
    stack = cf_client.describe_stacks(StackName=stack_name)['Stacks'][0]
    outputs = {output['OutputKey']: output['OutputValue'] for output in stack['Outputs']}

    # Retrieve job queue and job definition from CloudFormation outputs
    job_queue = outputs.get('JobQueue')
    job_definition = outputs.get('JobDefinition')

    if not job_queue or not job_definition:
        raise ValueError("JobQueue or JobDefinition not found in CloudFormation outputs")

    # Define the job parameters
    job_name = 'python-hello-world-job'

    # Submit the job
    response = batch_client.submit_job(
        jobName=job_name,
        jobQueue=job_queue,
        jobDefinition=job_definition,
        containerOverrides={
            'command': ["python3", "-c", "print('Hello, AWS Batch!')"]
        }
    )
    # Print the job ID
    print(f"Job submitted! Job ID: {response['jobId']}")
