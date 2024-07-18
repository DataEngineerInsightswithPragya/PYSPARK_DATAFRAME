def reverse_string(input_string):
    reverse_result = ""
    for ch in input_string:
        reverse_result = ch+reverse_result
    print("reverse_result : ", reverse_result)


input_string = "hello world"
reverse_string(input_string)


# def reverse_string(input_string):
#     reverse_result = input_string[::-1]
#     print("reverse_result : ", reverse_result)
#
#
# input_string = "hello world"
# reverse_string(input_string)