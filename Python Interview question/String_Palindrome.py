def is_palindrome(input_string):
    # Initialize two pointers, one at the start (l) and one at the end (r) of the string
    l = 0
    r = len(input_string) - 1

    # Loop until the two pointers meet in the middle
    while l < r:
        # Check if the character at the left pointer is alphanumeric
        # isalnum() returns True if the character is a letter or digit, else False
        if not input_string[l].isalnum():
            l += 1  # Move left pointer to the right
        # Check if the character at the right pointer is alphanumeric
        # isalnum() returns True if the character is a letter or digit, else False
        elif not input_string[r].isalnum():
            r -= 1  # Move right pointer to the left
        # Compare alphanumeric characters in a case-insensitive manner
        elif input_string[l].lower() == input_string[r].lower():
            l += 1  # Move left pointer to the right
            r -= 1  # Move right pointer to the left
        # If characters do not match, the string is not a palindrome
        else:
            return False
    # If all characters match, return True indicating the string is a palindrome
    return True

# Example usage:
input_string = "A man, a plan, a canal: Panama"
result = is_palindrome(input_string)
print(f"Is the string '{input_string}' a palindrome? {result}")
