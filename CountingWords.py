class CountingWords:
    """
    A class for counting words in text data.

    Methods:
    - count_words(corpus, filter=None): Count the occurrences of words in a given list of strings (corpus).
    - count_words_from_files(file_path, filter=None): Count the occurrences of words in a text file.
    """

    @staticmethod
    def count_words(corpus, filter=None):
        """
        Count the occurrences of words in a given list of strings (corpus).

        Args:
        - corpus (list): A list of strings to be analyzed.
        - filter (int, optional): A count threshold for filtering word count results. Only words with count greater than
          or equal to the filter value will be included in the results. Defaults to None (no filtering).

        Returns:
        - dict: A dictionary where keys are words and values are their respective counts.

        Raises:
        - TypeError: If corpus is not a list of strings.
        """
        # Validate input corpus
        assert type(corpus) is list, "The corpus to analyse needs to be a list of strings"

        # Combine all strings in corpus into one string
        corpus_text = ' '.join(corpus)

        # Split corpus text into words
        words = corpus_text.split()

        # Perform word count
        word_count = {}
        for word in words:
            if word in word_count:
                word_count[word] += 1
            else:
                word_count[word] = 1

        # Filter word count results if filter is provided
        if filter is not None:
            word_count = {word: count for word, count in word_count.items() if count >= filter}

        return word_count

    @staticmethod
    def count_words_from_files(file_path, filter=None):
        """
        Count the occurrences of words in a text file.

        Args:
        - file_path (str): The file path of the text file to be analyzed.
        - filter (int, optional): A count threshold for filtering word count results. Only words with count greater than
          or equal to the filter value will be included in the results. Defaults to None (no filtering).

        Returns:
        - dict: A dictionary where keys are words and values are their respective counts.

        Raises:
        - FileNotFoundError: If the specified file does not exist.
        """
        # Read contents from file
        try:
            with open(file_path, 'r') as file:
                corpus = file.read()
        except FileNotFoundError:
            raise FileNotFoundError("The specified file does not exist.")

        # Split corpus into words
        words = corpus.split()

        # Perform word count
        word_count = {}
        for word in words:
            if word in word_count:
                word_count[word] += 1
            else:
                word_count[word] = 1

        # Filter word count results if filter is provided
        if filter is not None:
            word_count = {word: count for word, count in word_count.items() if count >= filter}

        return word_count

    @staticmethod
    def filter_function(results, min_occurrences=4):
        """
        This method takes in a dictionary results containing word frequency counts and an optional
        integer argument min_occurrences that represents the minimum number of occurrences for a
        word to be included in the filtered dictionary. It returns a filtered dictionary containing
        only the key-value pairs where the value is greater than or equal to min_occurrences.
        """
        return {k: v for k, v in results.items() if v >= min_occurrences}
    
    @staticmethod
    def map_function(text):
        """
        This method takes in a string text and returns a generator that yields a tuple of the form
        (word, 1) for each word in the input string (with the word in lowercase). This is used as
        the mapping function in the MapReduce algorithm.
        """
        for word in text.lower().split():
            yield word, 1
            
    @staticmethod
    def apply_map(data):
        """
        This method takes in a list data containing strings and applies the map_function() to each
        string in the list. It returns a list of tuples containing the word and the count of 1 for
        each word in the input data.
        """
        results = list()
        for element in data:
            for map_result in CountingWords.map_function(element):
                results.append(map_result)
        return results
    
    @staticmethod
    def group_function(map_results):
        """
        This method takes in a list of tuples map_results containing the word and the count of 1 for
        each word, and groups the tuples by word. It returns a dictionary containing the word as the
        key and a list of counts as the value.
        """
        group_results = dict()
        for key, value in map_results:
            if key not in group_results:
                group_results[key] = [value]
            else:
                group_results[key].append(value)
        return group_results
    
    @staticmethod
    def reduce_function(key, values):
        """
        This method takes in a word key and a list of counts values and returns a tuple containing the
        word and the total count for that word.
        """
        total = 0
        for count in values:
            total += count
        return key, total
        
    @staticmethod
    def apply_reduce(group_results):
        """
        This method takes in a dictionary group_results containing the word as the key and a list of
        counts as the value, and applies the reduce_function() to each key-value pair in the dictionary.
        It returns a dictionary containing the word as the key and the total count as the value.
        """
        reduce_results = dict()
        for key, values in group_results.items():
            _, count = CountingWords.reduce_function(key, values)
            reduce_results[key] = count
        return reduce_results

# Emaxples
# Load some random text as string
text_1 = "Lorem ipsum dolor sit amet, consetetur et sadipscing elitr."
text_1b = "Lorem ipsum dolor sit amet, consetetur et sadipscing elitr."
text_2 = "At vero lorem et accusam et justo duo ipsum et ea rebum."
text_2b = "At vero lorem et accusam et justo duo ipsum et ea rebum. At vero lorem et accusam et justo duo ipsum et ea rebum."
data_set = [text_1, text_1b, text_2, text_2b] # combine in one variable

# create an instance of the class
counter = CountingWords()

# call the method on the instance
result = counter.count_words(data_set, filter=5)
print(result)

result_file = counter.count_words_from_files(r"C:/Users/stefa/OneDrive/Coding/Count Words/files/test.txt", filter=10)
print(result_file)
