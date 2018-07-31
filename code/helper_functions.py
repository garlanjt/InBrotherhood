
import os
from nltk.corpus import stopwords
from collections import Counter
import re
import string
from wordcloud import WordCloud
import numpy as np
import random
from PIL import Image




def grey_color_func(word, font_size, position, orientation, random_state=None,**kwargs):
    return "hsl(0, 0%%, %d%%)" % random.randint(60, 100)

def make_WordCloud(tweets,file,max_words,hashtags=False,both=False):
    #mask = np.array(Image.open(path.join(d, "stormtrooper_mask.png")))
    #stopwords = set(STOPWORDS)
    mask = np.array(Image.open( "../../data/wordCloud/falcons_mask_simple.png"))
    if hashtags:
        text = get_hastags(tweets)
    elif both:
        #This doesn't work becasue of the cursor going away
        text = get_hashtags_and_words(tweets)
    else:
        text = get_words(tweets)
    counts = Counter()
    counts.update(text.split())
    print(counts.most_common(max_words))
    text.replace("i","I")

    wc = WordCloud(background_color = "black",mask=mask,max_words=max_words,
                   contour_width=10, contour_color=(163,13,45),
                   stopwords=get_stop_words(), margin=10,
                   width=1000,height=1000,random_state=1,regexp=r"(#?\w+)").generate(text)
    wc.recolor(color_func=grey_color_func, random_state=3)
    #wc.to_file(file+".png")
    #os.system("open "+file+".png")
    return wc





def clean_up_text(tweet_text,RT=False):
    line = tweet_text
    line = re.sub(r'[.,"!]+', '', line, flags=re.MULTILINE)  # removes the characters specified
    if RT:
        line = re.sub(r'^RT[\s]+', '', line, flags=re.MULTILINE)  # removes RT
    line = re.sub(r'https?:\/\/.*[\r\n]*', '', line, flags=re.MULTILINE)  # remove link
    line = re.sub(r'[:]+', '', line, flags=re.MULTILINE)
    line = filter(lambda x: x in string.printable, line)  # filter non-ascii characers

    new_line = ''
    for i in line.split():  # remove @ and #words, punctuataion
        if not i.startswith('@') and not i.startswith('#') and i not in string.punctuation:
            new_line += i + ' '
    line = new_line
    return line



def unique_hashtags(tweets):
    hashtags = []
    for tweet in tweets:
        for hashtag in tweet['hashtags']:
            hashtags.append(hashtag.lower())
    print(len(set(hashtags)))
    #counts = Counter(hashtags)
    #print(counts)
def count_hashtags(tweets):
    hashtags = []
    for tweet in tweets:
        for hashtag in tweet['hashtags']:
            hashtags.append(hashtag.lower())
    counts = Counter(hashtags)
    print(counts)

def get_stop_words():
    punctuation = list(string.punctuation)
    stop = set(stopwords.words('english') + punctuation + ['rt', 'via','&amp;']) -set(["I","i","we","me","our"])
    return stop
def get_words(tweets):
    stop =get_stop_words()
    clean_text = []
    for tweet in tweets:
        clean_text += [term.lower() for term in clean_up_text(tweet["full_text"]).split() if term.lower() not in stop]
    big_string = ""
    for word in clean_text:
        big_string += word + " "

    return big_string

def get_words_limited_by_hashtags(tweets):
    stop =get_stop_words()

    clean_text = []
    for tweet in tweets:
        clean_text += [term.lower() for term in clean_up_text(tweet["full_text"]).split() if term.lower() not in stop]
    big_string = ""
    for word in clean_text:
        big_string += word + " "

    return big_string


def get_hastags(tweets):
    hashtags = []
    for tweet in tweets:
        hashtags += [term.lower() for term in tweet["hashtags"]]
    big_string = ""
    for word in hashtags:
        big_string += "#"+word.lower() + " "

    return big_string


def get_hashtags_and_words(tweets):
    stop =get_stop_words()
    clean_text = []
    for tweet in tweets:
        clean_text += [term.lower() for term in clean_up_text(tweet["full_text"]).split() if term.lower() not in stop]
        clean_text += [term.lower() for term in tweet["hashtags"]]
    big_string = ""
    for word in clean_text:
        big_string += word + " "
    return big_string


def count_words(tweets):
    counts = Counter()
    #<todo>This is unfortunatey removing words like "i","we","our",","me" which seem like they would be important
    punctuation = list(string.punctuation)
    stop = stopwords.words('english') + punctuation + ['rt', 'via','&amp;']

    for tweet in tweets:
        clean_text = [term.lower() for term in clean_up_text(tweet["full_text"]).split() if term.lower() not in stop]

        counts.update(clean_text)
    print(counts.most_common(20))


def get_hastags(tweet_list):
    hashtags = []
    for tweet in tweet_list:
        hashtags += [term.lower() for term in tweet["hashtags"]]
    big_string = ""
    for word in hashtags:
        big_string += "#"+word.lower() + " "

    return big_string