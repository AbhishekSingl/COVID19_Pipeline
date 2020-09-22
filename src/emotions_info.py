# NRC Classifier
from collections import defaultdict, Counter
import pandas as pd
import csv
from nltk.tokenize import TweetTokenizer
from Utilities import retrieve_file
import os

# Global Variables
FLAT_FILES_PATH = os.getenv('FLAT_FILES_PATH')


def emotion_count(tweet):
    """
    :param tweet: str; just one tweet
    :return: dictionary with 8 emotions and 2 sentiments corresponding to each word in the text
    """
    wordList = defaultdict(list)
    emotionList = defaultdict(list)
    f = retrieve_file(FLAT_FILES_PATH, "NRC-emotion-lexicon-wordlevel-alphabetized-v0.92.txt")
    reader = csv.reader(f, delimiter='\t')
    headerRows = [i for i in range(0, 2)]
    for _ in headerRows:
        next(reader)
    for word, emotion, present in reader:
        if int(present) == 1:
            wordList[word].append(emotion)
            emotionList[emotion].append(word)
    f.close()
    emo_count = Counter()
    for token in tweet:
        emo_count += Counter(wordList[token])

    return emo_count


def get_emotions(text):
    """
    :param text: a pandas series object with cleaned tweets i.e. no special characters.
    :return: returning a dataframe with new 10 columns i.e. anger, sad, joy, anticipation, fear, positive sentiment,
    negative sentiment ...
    """
    # Tokenization
    tt = TweetTokenizer()
    tokenized = text.apply(lambda x: tt.tokenize(x))
    emotion_info = [emotion_count(tweet) for tweet in tokenized]
    emotion_info = pd.DataFrame(emotion_info, index=tokenized.index).fillna(0)

    return emotion_info
