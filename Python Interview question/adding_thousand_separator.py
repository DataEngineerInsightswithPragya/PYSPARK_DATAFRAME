string = "1234567"
l = len(string)

cnt = 0
i = l - 1
new_string = ""

while i >= 0:
    new_string = string[i] + new_string
    i -= 1
    cnt += 1

    if cnt == 3 and i >= 0:
        new_string = ',' + new_string
        cnt = 0

print("new_string:", new_string)
