These python projects are the source code for the AWS Lambdas that route and parse incoming sensor data. They get data from AWS API Gateway, parse the data according to message version, write event logs and partial records (for v1 records) to S3, and insert sensor records directly to the Postgresql database in RDS.

To set up your machine:
- Make sure you have >= python 3.6 installed on your local machine, and set as the default python version. You can check your default python version with `python --version`.
- Install the python-lambda toolkit: https://github.com/nficano/python-lambda
- `boto3` wants AWS credentials in `~/.aws/credentials` on your machine. Contact Anya to get this credential info.

To run a lambda:
- Navigate to the directory of an individual lambda e.g. `cd routeMachineqData`
- Start the virtual environment `mkvirtualenv pylambda`
- Invoke the lambda `lambda invoke -v`
- To deploy `lambda deploy`


More invocation details:
By default, `lambda invoke -v` will look for a root-level file `event.json` as the test event. You can specify a different test file with the `--event-file` argument, e.g. `lambda invoke -v --event-file test_events/time_v1.json`
