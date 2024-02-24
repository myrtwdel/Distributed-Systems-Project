import random

a = [0, 3, 6, 9]
b = [round(random.uniform(0, 40), 1) for _ in range(10)]

result = [b[i] for i in a]
print(f" {b}")
print("Τα στοιχεία του b στις θέσεις που δίνει το a:")
print(result)