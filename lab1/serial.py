import re

def clean(inp):
    inp = inp.lower()
    matches = re.findall(r'(\b[^\s]+\b)', inp)
    return matches

import os 

count = {}

for f in os.listdir('data/'):
	data = clean(open(os.path.join(os.path.dirname(__file__), 'data', f),'r').read())
	for w in data:
		if w not in count:
			count[w] = 0
		count[w] += 1
print((sorted(count.items(), key=lambda item: item[1],reverse=True)[:10]))

