#
# -*- coding: latin-1 -*-
# This a python module implementation of the map-reduce that is based on the work of Dr. Gautam Shroff, find at http://www.lai18.com/content/1673980.html
# Some sanitization is achived, such as punctuation remove (except remove period between digits), html tags remove and whitespace remove.
# It is not provided remove whitespace inside the html tags title's.
# Future works: Other cleanup should be provided, but this is a work for pre-processing and data quality.
#
# Este módulo python é baseado no trabalho do Dr. Gautam Shroff, disponível em http://www.lai18.com/content/1673980.html
# Alguma limpeza é executada, como remoção de pontuação dos termos (não ocorre remoção do ponto entre dígitos), remoção de tags html e remoção de espaço em branco.
# Não é fornecida remoção de espaço em branco dentro de tags html.
# Trabalhos futuros: Outras limpezas podem ser implementadas. Porém este seria um trabalho para um pre-processamento e qualidade de dados (data quality)

import glob
import mincemeat
import operator

# The interested authors
G_ROZENBERG = "Grzegorz Rozenberg"
P_YU = "Philip S. Yu"


# reference to all files
ref_filespath = glob.glob('Trab2.3/*')

def file_contents(filename):
	f = open(filename)
	try:
		return f.read()
	finally:
		f.close()

# datasource for mincemeat server
datasource = dict((filename, file_contents(filename)) for filename in ref_filespath)

# ------------------------ mappers ---------------------
def mapper_fn(key, value):
	from stopwords import allStopWords		# stop words
	import re 								# regular expression
	for line in value.splitlines():
		words_of_line = line.split(':::')
		authors = words_of_line[1].split('::')
		
		# remove html tags from title: provided
		title = re.sub(r'<.*?>', '', words_of_line[2])

		# remove punctuation, except period between digits
		title = re.sub(r'\.(?!\d)', '', title).translate(None, ",;!?\"\'")

		# remove whitespace inside the html tags title's: not provided

		for author in authors:
			for term in title.split():
				word = term.strip().translate(None, "():").lower()
				if len(word)<=1 or word in allStopWords:
					continue
				yield (author, word), 1
                
# -------------------- reducer ----------------
def reducer_fn(key, value):
   result = sum(value)
   return result

# ------------------------- server ---------------
s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapper_fn
s.reducefn = reducer_fn

results = s.run_server(password="pwd")
#print results


# --- filtering the results by the interested search: in this case by the 2 authors ---------
# structure from results: (author, word): counter
# the counter is taking indexing the results by key: results[(author, word)]
rozenberg_occurs = list()
yu_occurs = list()
for key in results.keys():
	# process the next key from the dictionary
	if (G_ROZENBERG != key[0] and P_YU != key[0]):
		continue

	if key[0] == G_ROZENBERG:
		rozenberg_occurs.append((key[1], results[key]))
	elif key[0] == P_YU:
		yu_occurs.append((key[1], results[key]))

# sort each occurrences
rozenberg_occurs_sorted = sorted(rozenberg_occurs, key = operator.itemgetter(1, 0), reverse=True)
yu_occurs_sorted = sorted(yu_occurs, key = operator.itemgetter(1, 0), reverse=True)

# ------- get the top 2 occurrences for Grzegorz Rozenberg and Philip S. Yu  -------------
# top 2 words from Rozenberg
print "Top 2 words from " + G_ROZENBERG + ": "
print rozenberg_occurs_sorted[:2]
#print G_ROZENBERG + ": "
#print rozenberg_occurs_sorted

# top 2 from YU:
print "Top 2 words from " + P_YU + ": "
print yu_occurs_sorted[:2]
#print P_YU + ": "
#print yu_occurs_sorted

with open('top2-words-for.txt','w') as file:
	file.write(G_ROZENBERG + ': \n')
	for word, counter in rozenberg_occurs_sorted[:2]:
		file.write('\t' + word + ': ' + str(counter) + '\n')
	file.write(P_YU + ': \n')
	for word, counter in yu_occurs_sorted[:2]:
		file.write('\t' + word + ': ' + str(counter) + '\n')
# -------------------------------------------------------------
# all the words counted
resList=[(x[0],x[1],results[x]) for x in results.keys()]  
sorted_results = sorted(resList, key=operator.itemgetter(0,2))  
  
with open('output-all-words.txt','w') as f:  
        for (a,b,c) in sorted_results:  
                f.write(a +'*' + b+'*'+str(c)+'\n')
