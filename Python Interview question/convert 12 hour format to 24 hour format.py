def convert_time(time_str):
    # Extract AM/PM
    am_pm = time_str[-2:]

    print("am_pm --> ", am_pm)

    # Remove AM/PM from the string
    time_str = time_str[:-2]

    print("time_str --> ", time_str)

    # Split the time string into components
    parts = time_str.split('.')

    print("parts --> ", parts)

    # Extract hours, minutes, and seconds
    hour = int(parts[0])
    minute = int(parts[1])
    second = parts[2]

    # Convert hour to 24-hour format
    if am_pm == 'PM' and hour != 12:
        hour += 12
    elif am_pm == 'AM' and hour == 12:
        hour = 0

    # Format hour to two digits
    hour_24 = f"{hour:02}"

    # Construct the new time string in 24-hour format
    new_time_str = f"{hour_24}.{minute:02}.{second}"

    return new_time_str


# Example usage
time_str = "1.13.00PM"
converted_time = convert_time(time_str)
print(converted_time)  # Output: "13.13.00"
