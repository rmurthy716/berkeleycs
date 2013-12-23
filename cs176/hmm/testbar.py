#!/usr/bin/env python
import numpy as np
import matplotlib.pyplot as plt
from random import randint
from sys import argv

DIRECTORY = "../ps5data/"

def main(init, mu):
  f = open(DIRECTORY + "true_tmrca.txt")
  true = []
  for i in xrange(100000):
    true.append(float(f.readline()))
  f.close()

  f = open(DIRECTORY + 'decodings_' + init + '_' + mu + '.txt')
  vit = []
  post = []
  mean = []
  for line in f:
    if line[0] == "#":
      continue
    temp = line.split()
    vit.append(temp[0])
    post.append(temp[1])
    mean.append(temp[2])
  
  plt.plot(true, color='black')
  plt.plot(vit, color='green')
  plt.plot(post, color='blue')
  plt.plot(mean, color='red')

  plt.savefig(DIRECTORY + 'plot_' + init + '_' + mu + '.pdf')
  #plt.show()

def usage():
  print "python testbar.py <input>"
  exit(0)

if __name__ == '__main__':
  if len(argv) < 3:
    usage()
  main(argv[1], argv[2])
