s1 = 'pragyapragya'
s2 = 'gya'

cnt = 0

i = 0

while i <= len(s1) - len(s2):
    if s1[i:i + len(s2)] == s2:
        cnt = cnt + 1
        i = i + len(s2)
    else:
        i = i + 1
print("cnt --> ", cnt)
