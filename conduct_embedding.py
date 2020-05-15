import gensim
from gensim.models import KeyedVectors
import numpy as np

class MySentences(object):
    def __init__(self, dirname):
        self.dirname = dirname
 
    def __iter__(self):
        for line in open(self.dirname):
            yield line.split()
 
sentences = MySentences('.final_metains_data/final_data_0721.txt') # meta-path instances

word2freq = {}
for sentence in sentences:
    for word in sentence:
        if word not in word2freq.keys():
            word2freq[word] = 1
        else: word2freq[word] += 1

model = gensim.models.Word2Vec(sentences, min_count=10, size=50)
"""
sentences = [['this', 'is', 'the', 'first', 'sentence', 'for', 'word2vec'],
			['this', 'is', 'the', 'second', 'sentence'],
			['yet', 'another', 'sentence'],
			['one', 'more', 'sentence'],
			['and', 'the', 'final', 'sentence']]
model = gensim.models.Word2Vec(sentences, min_count=1)
"""
path = './word2vec_model'
model.save(path + '/model.bin')
print('model saved')
model.wv.save(path + 'vector.kv')
print('kv saved')

words = list(model.wv.vocab)

#out_word2index = open("./word2vec_model/word2index.txt", 'w')
out_index2word = open(path + "/index2word.txt", 'w')
out_index2type = open(path + "/index2type.txt", 'w')
out_index2freq = open(path + "/index2freq.txt", 'w')

for index, word in enumerate(words):
    out_index2word.write(str(index) + ',' + word + '\n')
    out_index2type.write(str(index) + ',' + word[0] + '\n')
    out_index2freq.write(str(index) + ',' + str(word2freq[word]) + '\n')
#out_word2index.close()
out_index2word.close()
out_index2type.close()
out_index2freq.close()

X = model[model.wv.vocab]
np.savetxt(path + '/embedding.txt', X, delimiter = " ", newline = "\n")
print('embedding saved')
"""
print(model['sentence'])
print("**************")
wv = KeyedVectors.load("./word2vec_model/vector.kv", mmap='r')
vector = wv['sentence']
print(vector)
"""