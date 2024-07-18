def single_number(input_list):
    result = 0
    for i in range(len(input_list)):
        result ^= input_list[i]
    print("result : ",result)


input_list = [2,4,7,7,4,3,2]
single_number(input_list)


# In this solution:
#
# We initialize single_number to 0.
# We iterate through the nums list and use the XOR operation (^=) to find the number that appears only once.
# XORing a number with itself cancels it out (because a ^ a = 0), so only the number that appears once remains
# in single_number after the loop.