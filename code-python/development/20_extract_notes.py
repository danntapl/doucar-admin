# # Extracting, Transforming, and Selecting Features

# **Note:** This file will not run as is.


# ## Removing stop words

# ### Examining the (hyper)parameters

print(remover.explainParams())

# ### Using a customized list of stop words:

my_stop_words = ["ian", "brian", "glynn"]
remover.setStopWords(my_stop_words)
remover.getStopWords()

# Now call the `fit` method.

# ### Using a localized list of stop words:

french_stop_words = StopWordsRemover.loadDefaultStopWords("french")
remover.setStopWords(french_stop_words)
remover.getStopWords()

# Now call the `fit` method.

