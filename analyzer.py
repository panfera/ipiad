import spacy
from spacy.lang.ru.examples import sentences
from sys import argv
script, text = argv

nlp = spacy.load("ru_core_news_sm")
doc = nlp(text)
print(doc.text)
for token in doc:
    print(token.text, token.pos_, token.dep_)