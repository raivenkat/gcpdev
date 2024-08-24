from pyspark import SparkContext, SparkConf

# Initialize SparkContext
conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)

# Load the text file into an RDD
text_file_path = "gs://dev-de-training-default/vrai/test/wordcount.txt"
text_rdd = sc.textFile(text_file_path)

# Perform word count
# 1. Split each line into words
words_rdd = text_rdd.flatMap(lambda line: line.split())

# 2. Map each word to (word, 1)
word_pairs_rdd = words_rdd.map(lambda word: (word, 1))

# 3. Reduce by key to count occurrences
word_count_rdd = word_pairs_rdd.reduceByKey(lambda count1, count2: count1 + count2)

# Collect and print results
print("Word Count:")
for word, count in word_count_rdd.collect():
    print(f"{word}: {count}")

# Stop the SparkContext
