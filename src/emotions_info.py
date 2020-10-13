# NRC Classifier
from collections import defaultdict, Counter
import pandas as pd
import csv
from nltk.tokenize import TweetTokenizer
from Utilities import retrieve_file
import os
import warnings
warnings.simplefilter('ignore')

# Global Variables
FLAT_FILES_PATH = os.getenv('FLAT_FILES_PATH')


class Emotions:
    def __init__(self):
        self.tt = TweetTokenizer()
        self.wordList = defaultdict(list)
        self.empty_df = pd.DataFrame(columns=['fear', 'trust', 'anger', 'disgust', 'negative',
                                              'positive', 'joy', 'anticipation', 'surprise', 'sadness'])
        self.emotion_count()

    def emotion_count(self):
        f = retrieve_file(FLAT_FILES_PATH, "NRC-emotion-lexicon-wordlevel-alphabetized-v0.92.txt")
        reader = csv.reader(f, delimiter='\t')
        headerRows = [i for i in range(0, 2)]
        for _ in headerRows:
            next(reader)
        for word, emotion, present in reader:
            if int(present) == 1:
                self.wordList[word].append(emotion)
        f.close()

    def emotion_parsing(self, text):
        text = self.tt.tokenize(text)
        emo_count = Counter()
        for token in text:
            emo_count += Counter(self.wordList[token])
        return emo_count

    def get_emotions(self, text):
        """
        :param text: a pandas series object with cleaned tweets i.e. no special characters.
        :return: returning a dataframe with new 10 columns i.e. anger, sad, joy, anticipation,
        fear, positive sentiment, negative sentiment ...
        """

        print(" Getting Emotions")
        emotion_info = pd.DataFrame(map(self.emotion_parsing, text), index=text.index)
        emotion_info = pd.concat([self.empty_df, emotion_info], axis=0)
        emotion_info.fillna(0, inplace=True)
        return emotion_info

