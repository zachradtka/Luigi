import luigi

"""
To run:
   python simple_pipeline.py --local-scheduler
"""

class InputFile(luigi.Task):
   """
   A Task wrapping up a Target object
   """

   def output(self):
      """
      Retun the contents of /etc/passwd
      """
      return luigi.LocalTarget('/etc/passwd')


class MyTask(luigi.Task):
   """
   
   """

   def requires(self):
      """
      Specity the dependency of the input file
      """
      return InputFile()

   def output(self):
      """
      Specify the file to write to
      """
      return luigi.LocalTarget('/tmp/userNames.txt')

   def run(self):
      """
      Run the Task
      """

      # Open the files specified by requires and output, respectively
      ifp = self.input().open('r')
      ofp = self.output().open('w')

      # Put each username in the output file
      for line in ifp:
         ofp.write('{0}\n'.format(line.strip().split(':')[0]))

      # Don't forget to close the file
      ofp.close()

if __name__ == '__main__':
   luigi.run(main_task_cls=MyTask)
