import luigi
import os
import shutil

OUTPUT_PATH = '/tmp/wordcount'
PARSED_FILE = 'parsed_file.txt'
SORTED_FILE = 'sorted_file.txt'
COUNT_FILE = 'wordcounts.txt'


def output_file(root_path, filename):
   return '{0}/{1}'.format(root_path, filename)

def nonblank_lines(lines):
   for line in lines:
      stripped_line = line.rstrip()

      if stripped_line:
         yield stripped_line

class BaseTask(luigi.Task):
   input_file = luigi.Parameter()


class InputFile(BaseTask):
   """
   A Task wrapping up a Target object
   """

   def output(self):
      """
      Retun the contents of /etc/passwd
      """
      return luigi.LocalTarget(self.input_file)

class ReadFile(BaseTask):

   def requires(self):
      return InputFile(input_file=self.input_file)

   def output(self):
      return luigi.LocalTarget(output_file(OUTPUT_PATH, PARSED_FILE))

   def run(self):
      ifp = self.input().open('r')
      ofp = self.output().open('w')

      for line in ifp:
         for word in line.strip().split():
            ofp.write('{0}\n'.format(word))

      ofp.close()


class SortFile(BaseTask):

   def requires(self):
      return ReadFile(input_file=self.input_file)

   def output(self):
      return luigi.LocalTarget(output_file(OUTPUT_PATH, SORTED_FILE))

   def run(self):

      # Read from the input file
      with self.input().open('r') as ifp:
         words = list(line.lower() for line in nonblank_lines(ifp))

      # Sort the words
      # TODO an in memory sort is not the best thing to do with a large file :)
      words.sort()

      # Create the output file to write to
      with self.output().open('w') as ofp:
         for word in words:
            ofp.write('{0}\n'.format(word))


class CountWords(BaseTask):

   def requires(self):
      return SortFile(input_file=self.input_file)

   def output(self):
      return luigi.LocalTarget(output_file(OUTPUT_PATH, COUNT_FILE))

   def run(self):

      count = 1
      curr_line = ''

      with self.output().open('w') as ofp:
         with self.input().open('r') as ifp:
            curr_line = ifp.readline()

            for line in ifp:
               if line.strip() == curr_line.strip():
                  count+=1

               else:
                  ofp.write('{0}\t{1}\n'.format(curr_line.strip(), count))
                  curr_line = line
                  count = 1

            ofp.write('{0}\t{1}\n'.format(curr_line.strip(), count))


if __name__ == '__main__':
   if os.path.exists(OUTPUT_PATH):
      shutil.rmtree(OUTPUT_PATH)

   luigi.run(['--local-scheduler', 
      '--task', 'CountWords', 
      '--input-file', '../../resources/MobyDick.txt'], 
      use_optparse=True)