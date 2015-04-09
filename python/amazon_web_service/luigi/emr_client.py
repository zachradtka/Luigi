import abc
import time
 
import boto
from boto.emr.connection import EmrConnection
from boto.regioninfo import RegionInfo
from boto.emr.step import PigStep
 
import luigi
from luigi.s3 import S3Target, S3PathTask
from luigi.contrib.pig import PigJobTask
 
from amazon_web_service.luigi import target_factory


import logging
 
logger = logging.getLogger('luigi-interface')


class EmrClient(object):


    # The Hadoop version to use
    HADOOP_VERSION = '1.0.3'

    # The AMI version to use
    AMI_VERSION = '2.4.7'
 
    # Interval to wait between polls to EMR cluster in seconds
    CLUSTER_OPERATION_RESULTS_POLLING_SECONDS = 10
 
    # Timeout for EMR creation and ramp up in seconds
    CLUSTER_OPERATION_RESULTS_TIMEOUT_SECONDS = 60 * 30
 
    def __init__(self, region_name='us-east-1', aws_access_key_id=None, aws_secret_access_key=None):
 
        # If the access key is not specified, get it from the luigi config.cfg file
        if not aws_access_key_id:
            aws_access_key_id = luigi.configuration.get_config().get('aws', 'aws_access_key_id')
 
        if not aws_secret_access_key:
            aws_secret_access_key = luigi.configuration.get_config().get('aws', 'aws_secret_access_key')
 
 
        # Create the region in which to run
        region_endpoint = u'elasticmapreduce.%s.amazonaws.com' % (region_name)
        region = RegionInfo(name=region_name, endpoint=region_endpoint)
 
        self.emr_connection = EmrConnection(aws_access_key_id=aws_access_key_id,
                                            aws_secret_access_key=aws_secret_access_key,
                                            region=region)
 
    def launch_emr_cluster(self, cluster_name, log_uri, ec2_keyname=None, master_type='m1.small', core_type='m1.small', num_instances=2, hadoop_version='1.0.3', ami_version='2.4.7', ):
 
        # TODO Remove
        # install_pig_step = InstallPigStep()
 
        jobflow_id = self.emr_connection.run_jobflow(name=cluster_name,
                              log_uri=log_uri,
                              ec2_keyname=ec2_keyname,
                              master_instance_type=master_type,
                              slave_instance_type=core_type,
                              num_instances=num_instances,
                              keep_alive=True,
                              enable_debugging=True,
                              hadoop_version=EmrClient.HADOOP_VERSION,
                              steps=[], 
                              ami_version=EmrClient.AMI_VERSION)
 
        # Log important information
        status = self.emr_connection.describe_jobflow(jobflow_id)

        logger.info('Creating new cluster %s with following details' % status.name)
        logger.info('jobflow ID:\t%s' % status.jobflowid)
        logger.info('Log URI:\t%s' % status.loguri)
        logger.info('Master Instance Type:\t%s' % status.masterinstancetype)
        
        # A cluster of size 1 does not have any slave instances
        if hasattr(status, 'slaveinstancetype'):
            logger.info('Slave Instance Type:\t%s' % status.slaveinstancetype)
        
        logger.info('Number of Instances:\t%s' % status.instancecount)
        logger.info('Hadoop Version:\t%s' % status.hadoopversion)
        logger.info('AMI Version:\t%s' % status.amiversion)
        logger.info('Keep Alive:\t%s' % status.keepjobflowalivewhennosteps)
 
        return self._poll_until_cluster_ready(jobflow_id)
 
 
    def add_pig_step(self, jobflow_id, pig_file, name='Pig Script', pig_versions='latest', pig_args=[]): 

        pig_step = PigStep(name=name,
                           pig_file=pig_file,
                           pig_versions=pig_versions,
                           pig_args=pig_args,
                           # action_on_failure='CONTINUE',
                       )

        self.emr_connection.add_jobflow_steps(jobflow_id, [pig_step])

        # Poll until the cluster is done working        
        return self._poll_until_cluster_ready(jobflow_id)


    def shutdown_emr_cluster(self, jobflow_id):
 
        self.emr_connection.terminate_jobflow(jobflow_id)
        return self._poll_until_cluster_shutdown(jobflow_id)
 
    def get_jobflow_id(self):
        # Get the id of the cluster that is WAITING for work
        return self.emr_connection.list_clusters(cluster_states=['WAITING']).clusters[0].id
 
    def _poll_until_cluster_ready(self, jobflow_id):
 
        start_time = time.time()
 
        is_cluster_ready = False
 
        while (not is_cluster_ready) and (time.time() - start_time < EmrClient.CLUSTER_OPERATION_RESULTS_TIMEOUT_SECONDS):
            # Get the state
            state = self.emr_connection.describe_jobflow(jobflow_id).state

            if state == u'WAITING':
                logger.info('Cluster intialized and is WAITING for work')
                is_cluster_ready = True

            elif (state == u'COMPLETED') or \
                 (state == u'SHUTTING_DOWN') or \
                 (state == u'FAILED') or \
                 (state == u'TERMINATED'):
                
                logger.error('Error starting cluster; status: %s' % state)

                # Poll until cluster shutdown
                self._poll_until_cluster_shutdown(jobflow_id)
                raise RuntimeError('Error, cluster failed to start')

            else:
                logger.debug('Cluster state: %s' % state)
                time.sleep(EmrClient.CLUSTER_OPERATION_RESULTS_POLLING_SECONDS)

 
        if not is_cluster_ready:
            # TODO shutdown cluster
            raise RuntimeError('Timed out waiting for EMR cluster to be active')
 
        return jobflow_id
 
 
    def _poll_until_cluster_shutdown(self, jobflow_id):
        start_time = time.time()
 
        is_cluster_shutdown = False
 
        while (not is_cluster_shutdown) and (time.time() - start_time < EmrClient.CLUSTER_OPERATION_RESULTS_TIMEOUT_SECONDS):
            # Get the state
            state = self.emr_connection.describe_jobflow(jobflow_id).state

            if (state == u'TERMINATED') or (state == u'COMPLETED'):
                logger.info('Cluster successfully shutdown with status: %s' % state)
                return False
            elif state == u'FAILED':
                logger.error('Cluster shutdown with FAILED status')
                return False
            else:
                logger.debug('Cluster state: %s' % state)
                time.sleep(EmrClient.CLUSTER_OPERATION_RESULTS_POLLING_SECONDS)

        if not is_cluster_shutdown:
            # TODO shutdown cluster
            raise RuntimeError('Timed out waiting for EMR cluster to shut down')
 
        return True


class EmrTask(luigi.Task):
 
    @abc.abstractmethod
    def output_token(self):
        """
        Luigi Target providing path to a token that indicates completion of this Task.

        :rtype: Target:
        :returns: Target for Task completion token
        """
        raise RuntimeError("Please implement the output_token method")
 
    def output(self):
        """
        The output for this Task. Returns the output token by default, so the task only runs if the 
        token does not already exist.

        :rtype: Target:
        :returns: Target for Task completion token
        """
        return self.output_token()


class InitializeEmrCluster(EmrTask):
    """
    Luigi Task to initialize a new EMR cluster.

    This Task writes an output token to the location designated by the `output_token` method to 
    indicate that the clustger has been successfully create. The Task will fail if the cluster
    cannot be initialized.

    Cluster creation in EMR takes between several seconds and several minutes; this Task will
    block until creation has finished.
    """
 
    # The s3 URI to write logs to, ex: s3://my.bucket/logs 
    log_uri = luigi.Parameter()
    
    # The Key pair name of the key to connect 
    ec2_keyname = luigi.Parameter(None)
    
    # The friendly name for the cluster
    cluster_name = luigi.Parameter(default='EMR Cluster')
    
    # The EC2 type to use for the master
    master_type = luigi.Parameter(default='m1.small')
    
    # The EC2 type to use for the slaves
    core_type = luigi.Parameter(default='m1.small')
    
    # The number of instances in the cluster
    num_instances = luigi.IntParameter(default=1)
    
    # The version of hadoop to use
    hadoop_version = luigi.Parameter(default='1.0.3')
    
    # The AMI version to use
    ami_version = luigi.Parameter(default='2.4.7')


    def run(self):
        """
        Create the EMR cluster
        """
 
        emr_client = EmrClient()
        emr_client.launch_emr_cluster(ec2_keyname=self.ec2_keyname,
                                      log_uri=self.log_uri,
                                      cluster_name=self.cluster_name, 
                                      master_type=self.master_type,
                                      core_type=self.core_type,
                                      num_instances=self.num_instances,
                                      hadoop_version=self.hadoop_version,
                                      ami_version=self.ami_version)

        target_factory.write_file(self.output_token())


class TerminateEmrCluster(EmrTask):
    
    def run(self):

        emr_client = EmrClient()
        jobflow_id = emr_client.get_jobflow_id()

        emr_client.shutdown_emr_cluster(jobflow_id)

        target_factory.write_file(self.output_token())

class EmrPigTask(EmrTask):

    # The absolute path to the root of the pigscript directory
    pig_path = luigi.Parameter()

    def run(self):

        emr_client = EmrClient()
        jobflow_id = emr_client.get_jobflow_id()

        logger.debug('Adding task to jobflow: %s' % jobflow_id)

        pig_args=self.pig_args()


        emr_client.add_pig_step(jobflow_id=jobflow_id, 
                                pig_file=self.pig_path,
                                pig_args=pig_args)

    @abc.abstractmethod
    def pig_args(self):
        """
        List of args to tell the pig task how to run

        :rtype: List:
        :returns: list of args for the pig task
        """
        raise RuntimeError("Please implement the build_args method")
