# A decorator is essentially a callable (like a function) that takes another function as an argument
# and returns a new function that usually extends the behavior of the original function.

def my_decorator(func):
    def fun():
        print("good morning pragya")
        func()
        print("bye")
    return fun


@my_decorator
def hello():
    print("hello world")

hello()