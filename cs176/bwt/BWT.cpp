/* Implementation of q1 problem set 2
   cs176
*/

#include <cstdio>
#include <iostream>
#include <algorithm>
#include <vector>
#include <cmath>
#include <string>
#include <set>
#include <map>
#include <ctime>
#include <cstring>
#include <cassert>
#include <sstream>
#include <iomanip>
#include <complex>
#include <queue>
#include <functional>
#include <fstream>
using namespace std;
#define VAR(a,b) __typeof(b) a=(b)
#define FOR(i,a,b) for (int _n(b), i(a); i < _n; i++)
#define FORD(i,a,b) for(int i=(a),_b=(b);i>=_b;--i)
#define FOREACH(it,c) for(VAR(it,(c).begin());it!=(c).end();++it)
#define REP(i,n) FOR(i,0,n)
#define ALL(c) (c).begin(), (c).end()
#define SORT(c) sort(ALL(c))
#define REVERSE(c) reverse(ALL(c))
#define UNIQUE(c) SORT(c),(c).resize(unique(ALL(c))-(c).begin())
#define MAX(a, b) a = max(a,b)
#define MIN(a, b) a = min(a,b)

#define GetI() (SA12[t] < n0 ? SA12[t] * 3 + 1 : (SA12[t] - n0) * 3 + 2)
#define PWIDTH 80
#define ASCIIRANGE 124

int M [ASCIIRANGE];

inline bool lex(int a1, int a2, int b1, int b2) {
  return (a1 < b1 || a1 == b1 && a2 < b2);
}

inline bool lex(int a1, int a2, int a3, int b1, int b2, int b3) {
  return (a1 < b1 || a1 == b1 && lex(a2, a3, b2, b3));
}

static inline void dump(int a[], int n) {
  int i = 0;
  fprintf(stderr, "line-dump: ");
  while (i < n) fprintf(stderr, "%d ", a[i++]);
  fprintf(stderr, "\n");
}

void usage() {
  printf("Usage: prob1 (-bwt|-ibwt) input output\n");
  exit(1);
}

void notfound() {
  printf("Error: Input file not found\n");
  exit(1);
}

void radixPass(int* a, int* b, int* r, int n, int K) {
  int* c = new int[K+1];
  int i;
  REP(i, K+1) c[i] = 0;
  REP(i, n) c[r[a[i]]]++;
  int sum = 0;
  REP(i, K+1) {
    int t = c[i];
    c[i] = sum;
    sum += t;
  }
  REP(i, n) b[c[r[a[i]]]++] = a[i];
  delete [] c;
}

void suffixArray(int* T, int* SA, int n, int K) {
  int n0 = (n + 2)/3; 
  int n1 = (n + 1)/3;
  int n2 = n/3;
  int n02 = n0 + n2;
  int* R = new int[n02 + 3];
  
  //Extra locations should be zeroed out
  R[n02] = R[n02 + 1] = R[n02 + 2] = 0;
  int* SA12 = new int[n02 + 3]; 
  SA12[n02] = SA12[n02 + 1] = SA12[n02 + 2] = 0;
  int* R0 = new int[n0];
  int* SA0 = new int[n0];

  //Initialize mod1 and mod2 indices
  for (int i = 0, j = 0; i < n + (n0 - n1); i++) if (i%3 != 0) R[j++] = i;

  radixPass(R, SA12, T+2, n02, K);
  radixPass(SA12, R, T+1, n02, K);
  radixPass(R, SA12, T, n02, K);

  int name = 0, c0 = -1, c1 = -1, c2 = -1;
  for (int i = 0; i < n02; i++) {
    if (T[SA12[i]] != c0 || T[SA12[i]+1] != c1 || T[SA12[i]+2] != c2) {
      name++;
      c0 = T[SA12[i]];
      c1 = T[SA12[i]+1];
      c2 = T[SA12[i]+2];
    }
    if (SA12[i] % 3 == 1)
      R[SA12[i]/3] = name;
    else
      R[SA12[i]/3 + n0] = name;
  }

  if (name < n02) {
    suffixArray(R, SA12, n02, name);
    // store unique names in R using the suffix array
    for (int i = 0; i < n02; i++) R[SA12[i]] = i + 1;
  } 
  else for (int i = 0; i < n02; i++) SA12[R[i] - 1] = i;

  //Sort 0 mod 3
  for (int i = 0, j = 0; i < n02; i++) 
    if (SA12[i] < n0) R0[j++] = 3*SA12[i];
  radixPass(R0, SA0, T, n0, K);

  for (int p = 0, t = n0-n1, k = 0; k < n; k++) {
    int i = GetI(); // pos of current offset 12 suffix
    int j = SA0[p]; // pos of current offset 0 suffix
    if (SA12[t] < n0 ? // different compares for mod 1 and mod 2 suffixes
	lex(T[i], R[SA12[t] + n0], T[j], R[j/3]) :
	lex(T[i], T[i + 1], R[SA12[t] - n0 + 1], T[j], T[j + 1], R[j/3 + n0])) {
      SA[k] = i; t++;
      if (t == n02)
	for (k++; p < n0; p++, k++) SA[k] = SA0[p];
    } 
    else {
      SA[k] = j; p++;
      if (p == n0)
	for (k++; t < n02; t++, k++) SA[k] = GetI();
    }
  }
  delete [] R; delete [] SA12; delete [] SA0; delete [] R0;
}

int mod(int a, int n) {
  if (a < 0) return a + n;
  else return a%n;
}

static void outputBWT(int* SA, string S, ofstream& stream) {
  stream << ">BWT" << endl;
  for(int i = 0; i < S.length(); i++) {
    stream << S[mod(SA[i]-1, S.length())];
    if (i % PWIDTH == PWIDTH-1) stream << endl;
  }
  stream << endl;
}

static void outputiBWT(int* N, int* M, string S, ofstream& stream) {
  stream << ">iBWT" << endl;
  int i = 0;
  char* ret = new char[S.length()];
  int b = N[i] + M[S[i]];
  ret[i++] = '$';
  ret[i++] = S[0];
  while (true) {
    if (S[b] != '$') ret[i++] = S[b];
    else break;
    b = N[b] + M[S[b]];
  }
  for(int j = 0, i = S.length()-1; i >= 0; i--, j++) {
    stream << ret[i];
    if (j % PWIDTH == PWIDTH-1) stream << endl;
  }
  stream << endl;
  delete [] ret;
}

void performBWT(string S, ofstream& stream) {
  int* input = new int[S.length() + 3];
  int* SA = new int[S.length() + 3];
  int i, m = -1;
  REP(i, S.length()) {
    input[i] = (int) S[i];
    MAX(m, input[i]);
  }
  FOR(i, S.length(), S.length() + 3) SA[i] = input[i] = 0;
  suffixArray(input, SA, S.length(), m);
  outputBWT(SA, S, stream);
  delete [] input; delete [] SA;
}

void performiBWT(string S, ofstream& stream) {
  int* N = new int [S.length()];
  memset(M, 0, ASCIIRANGE);
  memset(N, 0, S.length());
  REP(i, S.length()) {
    N[i] = M[S[i]];
    M[(int)S[i]]++;
  }
  int sum = 0;
  REP(i, ASCIIRANGE) {
    int t = M[i];
    M[i] = sum;
    sum += t;
  }
  outputiBWT(N, M, S, stream);
  delete [] N;
}

int main(int argc, char** argv) {
  if (argc != 4) usage();
  if (!(strcmp(argv[1], "-bwt") == 0 || strcmp(argv[1], "-ibwt") == 0))
    usage();
  ifstream inp;
  inp.open(argv[2]);
  if (!inp.is_open()) notfound();

  string temp, S, state;
  inp >> state >> temp;
  while(inp) {
    S.append(temp);
    inp >> temp;
  }

  ofstream outf (argv[3]);
  if (strcmp(argv[1], "-bwt") == 0) performBWT(S, outf);
  else performiBWT(S, outf);
  return 0;
}
