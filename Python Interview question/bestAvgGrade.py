def bestAverageGrade(scores):
    if not scores:
        return 0

    # Dictionary to store total score and number of tests for each student
    student_scores = {}

    # Populate the dictionary with scores
    for name, score in scores:
        if name not in student_scores:
            student_scores[name] = []
        student_scores[name].append(int(score))

    # Calculate the average score for each student
    best_avg = float('-inf')
    for scores in student_scores.values():
        avg_score = sum(scores) // len(scores)  # Integer division to floor the average
        best_avg = max(best_avg, avg_score)

    return best_avg


# Example usage:
scores = [
    ["Bobby", "87"],
    ["Charles", "100"],
    ["Eric", "64"],
    ["Charles", "22"]
]

print(bestAverageGrade(scores))  # Output: 87
