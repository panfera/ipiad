import stanza
from sys import argv
script, text = argv

#f = open(first,'r',encoding='utf-8')
#text = f.read()
#print(text)

f_w = open("output.txt", 'w', encoding='utf-8')

stanza.download('ru')
nlp = stanza.Pipeline('ru')
doc = nlp(text)
f_w.write(str(doc))
f_w.write(str(doc.entities))
#f.close()
f_w.close()