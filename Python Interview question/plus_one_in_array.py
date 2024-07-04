def add_one(digits):
    # Finding the length of the array and setting the index to the last element
    i = len(digits) - 1

    # Looping from the last element to the first
    while i >= 0:
        # If the current digit is less than 9, increment it by 1 and return the result
        if digits[i] < 9:
            digits[i] += 1
            return digits
        else:
            # If the current digit is 9, set it to 0 and continue the loop
            digits[i] = 0

        # Move to the previous digit
        i = i - 1

    # If all digits were 9, the loop finishes and we prepend 1 to the array
    return [1] + digits


if __name__ == "__main__":
    # Example list of digits representing the number 999
    digits = [9, 9, 9]
    # Adding one to the number represented by the list
    final_results = add_one(digits)
    # Printing the final result after adding one
    print("final_results:", final_results)
