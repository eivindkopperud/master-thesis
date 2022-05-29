import os
from sys import argv
import numpy as np
from statistics import variance,mean
# https://numpy.org/doc/stable/reference/generated/numpy.linalg.lstsq.html

path = argv[1]
pwd = os.environ["PWD"]+"/"
list_of_files = []
for root, dirs, files in os.walk(pwd + path):
    for file in files:
        list_of_files.append(os.path.join(root,file))


Q1 = []
Q2 = []
Q3 = []
Q4 = []

LN = []
UN = []
dists = [LN,UN]
list_of_qs = [Q1,Q2,Q3,Q4]
list_of_stigningstall = []
def calcStuff(filepath):
    f = open(filepath, "r")
    lines = f.readlines()
    x = []
    y = []
    for line in lines[1:]:
        s = line.strip().split(",")
        x.append(int(s[-1]))
        y.append(int(s[1]))

    # Usikker på hva denne gjør, men den gjør at den funker
    y = [num / max(y) for num in y]
    A = np.vstack([x,np.ones(len(x))]).T

    m, _ = np.linalg.lstsq(A,y, rcond=None)[0]

    #print(f"{argv[1]}: ({m=}, {c=})")
#    print(m)
    if "LogNormal" in file:
        LN.append(m)
    if "Uniform" in file:
        UN.append(m)
    f.close()

list_of_files.sort()
for file in list_of_files:
    calcStuff(file)

print(dists)

