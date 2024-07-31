string = '100,000,000.90'

l = len(string)

new_string = ''

for i in range(l):
	if string[i] == ',':
		new_string +='.'
	elif string[i] == '.':
		new_string +=','
	else:
	    new_string += string[i]

print(new_string)

#
# output:
# 100.000.000,90