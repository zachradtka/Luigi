
-- Set the default input and output root paths
%default INPUT_ROOT_PATH 's3://zach.radtka.data'
%default OUTPUT_ROOT_PATH 's3://zach.radtka.output'

-- Set the input and output paths for this wordcount script
%default INPUT_BOOKS '$INPUT_ROOT_PATH/books/*'
%default OUTPUT_PATH '$OUTPUT_ROOT_PATH/WORD_COUNT'

-- Load the text from the books
raw_text = LOAD '$INPUT_BOOKS';

-- Tokenize each line and write a single word to each line
single_word = FOREACH raw_text GENERATE FLATTEN(TOKENIZE($0)) as word;

-- Group all words that are similar
word_groups = GROUP single_word by word;

-- Count the number of occurrences of each word
word_counts = FOREACH word_groups GENERATE group, COUNT(single_word);

-- Remove the output path if it already exists
rmf $OUTPUT_PATH;

-- Store the word count into the output path
STORE word_counts INTO '$OUTPUT_PATH'using PigStorage();