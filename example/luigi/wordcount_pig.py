import luigi
from luigi import configuration
from luigi.contrib.pig import PigJobTask
from luigi.s3 import S3Target, S3PathTask

from amazon_web_service.luigi.emr_client import InitializeEmrCluster
from amazon_web_service.luigi.emr_client import TerminateEmrCluster

from amazon_web_service.luigi.emr_client import EmrPigTask

import logging
logger = logging.getLogger('luigi-interface')

import inspect
import os
import re

"""
   To run:
      Required Parameters:
         --project-root: The absolute path to your project. No trailing slash.

   Optional Parameters
      --output-root-path: The root location where output will be stored. No trailing slash.
      --input-root-path: The root loation for all input files. No trailing slash.

   Ex:
      python wordcount_pig.py --local-scheduler --project-root <Absolute path to your project> --input-root-path <Absolute path to input files> --output-root-path <Absolute path to output files>

"""

# Number of reduce task slots per m1.xlarge instance
NUM_REDUCE_SLOTS_PER_MACHINE = 3

def create_task_s3_target(root_path, task_str):
   """
   Helper method for easily creating S3Targets
   """
   return S3Target('%s/%s' % (root_path, task_str))

def create_task_local_target(root_path, task_str):
   """
   Helper method for easily creating local targets
   """
   return luigi.LocalTarget("%s/%s" % (root_path, task_str))


class LocalInputFile(luigi.Task):
   """
   A Task wrapping up a Target object
   """
   path = luigi.Parameter()

   def output(self):
      """
      Retun the Target path
      """
      return luigi.LocalTarget(self.path)


class PigscriptTask(PigJobTask):

   # The root path to where input data is located
   input_root_path = luigi.Parameter()

   # The root path to where output data will be written, a local or S3 path
   output_root_path = luigi.Parameter()

   # The absolute path to the root of the pigscript directory
   project_root = luigi.Parameter()
   
   # The cluster size to use for running job
   cluster_size = luigi.IntParameter(default=10)

   # A parameter to allow the job to run locally
   run_locally = luigi.BoolParameter(default=False)

   # 
   root_path = None

   def pig_env_vars(self):
      return { "PIG_CLASSPATH":'%(root)s/.:%(root)s/lib-cluster/*:%(root)s/lib-pig/*:%(root)s/jython/jython.jar' % \
            {'root': self._get_root_path()}
   }
     
   def pig_properties(self):
      """
      Define pig specific properties that will be passed to all of the subclasses of 
      PigTaskConfiguration
      """

      props = {
         'pig.logfile': self._logfile(),
         'pig.exec.reducers.bytes.per.reducer':268435456,
         'fs.default.name':'file:///',
         'pig.temp.dir':'/tmp/',
      }

      # Add s3 properties if running remotely
      if not self.run_locally:
         props.update({
            'fs.s3.impl':'org.apache.hadoop.fs.s3native.NativeS3FileSystem',
            'fs.s3n.awsAccessKeyId':self._get_aws_access_key(),
            'fs.s3n.awsSecretAccessKey':self._get_aws_secret_access_key(),
            'fs.s3.awsAccessKeyId':self._get_aws_access_key(),
            'fs.s3.awsSecretAccessKey':self._get_aws_secret_access_key(),
         })

      return props
     
   def pig_parameters(self):
      """
      Define pramaters that will be passed to all the subclasses of PigTaskConfiguration
      """

      # Parameters required to run the job locally
      params = {}

      # Add s3 properties if running remotely
      if not self.run_locally:
         params.update({
            'aws_access_key_id':self._get_aws_access_key(),
            'AWS_ACCESS_KEY_ID':self._get_aws_access_key(),
            'AWS_ACCESS_KEY':self._get_aws_access_key(),
            'aws_secret_acces_key':self._get_aws_secret_access_key(),
            'AWS_SECRET_KEY':self._get_aws_secret_access_key(),
            'AWS_SECRET_ACCESS_KEY':self._get_aws_secret_access_key(),
         })

      # Add any parameters defined in the specific task
      params.update(self.parameters())

      return params

   def parameters(self):
      """
      Dictionary of parameters that should be set for the Pig job.

      Ex::
         return { 'YOUR_PARAM_NAME':'Your param value' }
      """

      return {}

   def pig_options(self):
      """
      Define the options that will be passed all the subclasses of PigTaskConfiguration
      """

      # Set the path to the log4j conf file
      options = ['-log4jconf', self._log4j_conf()]

      # Run the job locall if the run-locally option is set
      if self.run_locally:
         options.extend(['-x', 'local'])

      return options

   def pig_script_path(self):
      """
      Return the absolute path to the pig script to run
      """

      return "%s/pigscripts/%s" % (self.project_root, self.script())

   def script(self):
      """
      Return the name of the Pig script to run
      """

      raise NotImplementedError("subclass should define script")

   def output(self):
      """
      Return the output path of the script
      """

      return self.script_output()

   def script_output(self):
      """
      
      List of output paths generated by the pigscript

      Ex::
         return ['output_path']
      """

      raise NotImplementedError("subclass should define the script output path")

   def books_file(self):
      """
      Return 'aws_access_key_id' from client.cfg
      """
      return "%s/%s" % (self.input_root_path, 'books/MobyDick.txt')

   def create_s3_target(self, task_str):
      return create_task_s3_target(self.output_root_path, task_str)

   def create_local_target(self, task_str):
      return create_task_local_target(self.output_root_path, task_str)

   def _get_aws_access_key(self):
      """
      Return 'aws_access_key_id' from client.cfg
      """

      return configuration.get_config().get('aws', 'aws_access_key_id')

   def _get_aws_secret_access_key(self):
      """
      Return 'aws_secret_access_key' from client.cfg
      """

      return configuration.get_config().get('aws', 'aws_secret_access_key')

   def _log4j_conf(self):
      """
      Return the logging properties for log4j
      """

      return '%s/conf/log4j-cli-local-dev.properties' % (self.project_root);

   def _logfile(self):
      """
      The the location to store the pig logs
      """

      return '%s/logs/local-pig.log' % (self.project_root)

   def _get_root_path(self):
      """
      Return the root path of the project
      """

      if not self.root_path:
         self.root_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

      return self.root_path

   def _s3_safe(self, s):
      """
      Ensure the S3 path specified is syntatically correct
      """
      return re.sub("[^0-9a-zA-Z]", '-', s)


class WordCount(PigscriptTask):

   def requires(self):
      """
      The requires method is how you build your dependency graph in Luigi.  Luigi will not
      run this task until all tasks returning in this list are complete.

      S3PathTask is a simple Luigi task that ensures some path already exists. As
      GenerateUserSignals is the first task in this Luigi pipeline it only requires that its
      expected input data can be found.
      """
      if self.run_locally:
         return [LocalInputFile(self.books_file())]
      else:
         return [S3PathTask(self.books_file())]

   def script_output(self):
      """
      The script_output method defines where output from the task will be stored.

      Luigi will check this output location before starting any tasks that depend on this task.
      """

      if self.run_locally:
         return [self.create_local_target('wordcount')]
      else:
         return [self.create_s3_target('wordcount')]

   def parameters(self):
      """
      This method defines the parameters that will be passed to the pig script when starting
      this pigscript.
      """

      return {
         'OUTPUT_ROOT_PATH': self.output_root_path,
         'INPUT_ROOT_PATH' : self.input_root_path,
      }

   def script(self):
      """
      The name of the pigscript to run
      """
      return "wordcount.pig"



class CreateEmrCluster(InitializeEmrCluster):
    """
    This task creates an EMR cluster for runnig the rest of the tasks on
    """
 
    output_root_path = luigi.Parameter()
 
    def output_token(self):
        return create_task_s3_target(self.output_root_path, 
                                     self.__class__.__name__)


class RemoteWordCount(EmrPigTask):

   # The root path to where input data is located
   input_root_path = luigi.Parameter()

   # The root path to where output data will be written, a local or S3 path
   output_root_path = luigi.Parameter()


   def pig_args(self):
      args = []
      args.extend(['-p','INPUT_ROOT_PATH=%s' % self.input_root_path])
      args.extend(['-p','OUTPUT_ROOT_PATH=%s' % self.output_root_path])
      return args

   def output_token(self):

      return create_task_s3_target(self.output_root_path, 
                                   self.__class__.__name__)


class ShutdownEmrCluster(TerminateEmrCluster):
    """
    This task terminates an EMR cluster
    """
 
    output_root_path = luigi.Parameter()
 
    def output_token(self):
        return create_task_s3_target(self.output_root_path, 
                                     self.__class__.__name__)


if __name__ == "__main__":
   """
   We tell Luigi to run the last task in the task dependency graph.  Luigi will then
   work backwards to find any tasks with its requirements met and start from there.

   The first time this pipeline is run the only tasks with the requirements met will be
   the GenerateUserSignals and GenerateItemSignals tasks which will find their
   required input files in S3.
   """
   # luigi.run(main_task_cls=WordCount)
   #luigi.run(main_task_cls=CreateEmrCluster)
   # luigi.run(main_task_cls=RemoteWordCount)
   luigi.run(main_task_cls=ShutdownEmrCluster)
