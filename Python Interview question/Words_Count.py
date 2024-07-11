# Define a function named word_count that takes one argument, 'paragraph_words'.
def word_count(paragraph_words):
    d = {}
    word_splitted = paragraph_words.split(" ")
    print("word_splitted ----- : ",word_splitted)

    for i in word_splitted:
        if i not in d:
            d[i] = 1
        else:
            d[i] = d[i]+1
    print("d --------  : ",d)


# Call the word_count function with an input sentence and print the results.
word_count('the quick brown box python fox jumps over python the lazy dog and python')