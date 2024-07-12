def two_sum(nums, target):
    # Create a dictionary to store the number and its index
    num_dict = {}

    for i in range(len(nums)):
        # Calculate the difference needed to reach the target
        diff = target - nums[i]

        # If the difference is already in the dictionary, we found a pair
        if diff in num_dict:
            return [num_dict[diff], i]

        # Otherwise, store the index of the current number in the dictionary
        num_dict[nums[i]] = i

    # If no pair is found, return an empty list or raise an exception
    return []


# Example usage:
nums = [2, 7, 11, 15]
target = 9
print(two_sum(nums, target))  # Output: [0, 1]
