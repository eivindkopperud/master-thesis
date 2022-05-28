from sys import argv
import numpy as np
# https://numpy.org/doc/stable/reference/generated/numpy.linalg.lstsq.html

f = open(argv[1], "r")
lines = f.readlines()
x = []
y = []
for line in lines[1:]:
    s = line.split(",")
    x.append(int(s[-1]))
    y.append(int(s[1]))

# Usikker på hva denne gjør, men den gjør at den funker
A = np.vstack([x,np.ones(len(x))]).T

m, c = np.linalg.lstsq(A,y, rcond=None)[0]

print(f"Stigningstall {m=}")
print(f"restledd {c=}") # Husker ikke hva denne heter


f.close()
