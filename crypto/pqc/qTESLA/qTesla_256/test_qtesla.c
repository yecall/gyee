#include <stdio.h>
#include <stdlib.h>
#include "rng.h"
#include "cpucycles.h"
#include "api.h"
#include "sign.h"
#include "poly.h"


#define MLEN 59
#define NRUNS 10000
#define NRUNS_GEN 10
#define NRUNS_SIGN 5000
#define NRUNS_VERIF 10000

//int get_sig_rejections(void);

static int cmp_llu(const void *a, const void*b)
{
  if(*(unsigned long long *)a < *(unsigned long long *)b) return -1;
  if(*(unsigned long long *)a > *(unsigned long long *)b) return 1;
  return 0;
}


static unsigned long long median(unsigned long long *l, size_t llen)
{
  qsort(l,llen,sizeof(unsigned long long),cmp_llu);

  if(llen%2) return l[llen/2];
  else return (l[llen/2-1]+l[llen/2])/2;
}

static unsigned long long average(unsigned long long *t, size_t tlen)
{
  unsigned long long acc=0;
  size_t i;
  for(i=0;i<tlen;i++)
    acc += t[i];
  return acc/(tlen);
}

static void print_results(const char *s, unsigned long long *t, size_t tlen)
{
  size_t i;
  printf("%s", s);
  for(i=0;i<tlen-1;i++)
  {
    t[i] = t[i+1] - t[i];
    /*printf("%llu ", t[i]);*/
  }
  printf("\n");
  printf("median:  %llu\n", median(t, tlen));
  printf("average: %llu\n", average(t, tlen-1));
  printf("\n");
}


unsigned char mi[MLEN];
unsigned char mo[MLEN+CRYPTO_BYTES];
unsigned char sm[MLEN+CRYPTO_BYTES];
unsigned char pk[CRYPTO_PUBLICKEYBYTES];
unsigned char sk[CRYPTO_SECRETKEYBYTES];
unsigned long long smlen, mlen;




unsigned long long t[NRUNS];

int main()
{
  int i,j;

  FILE *urandom = fopen("/dev/urandom", "r");
  printf("CRYPTO_PUBLICKEYBYTES: %lu\n",CRYPTO_PUBLICKEYBYTES);
  printf("CRYPTO_SECRETKEYBYTES: %lu\n",CRYPTO_SECRETKEYBYTES);
  printf("CRYPTO_BYTES: %d\n\n",CRYPTO_BYTES);

  for(j=0;j<NRUNS_GEN;j++)
  {
    t[j] = cpucycles();
    crypto_sign_keypair(pk, sk);
  }
  print_results("crypto_sign_keygen: ",t,NRUNS_GEN);
  randombytes(mi,MLEN);
  for(i=0;i<NRUNS_SIGN;i++)
  {
    t[i] = cpucycles();
    crypto_sign(sm, &smlen, mi, MLEN, sk);
    
  }
  print_results("crypto_sign: ", t, NRUNS_SIGN);

  for(i=0;i<NRUNS_VERIF;i++)
  {
    t[i] = cpucycles();
    crypto_sign_open(mo, &mlen, sm, smlen, pk);
  }
  print_results("crypto_sign_open: ", t, NRUNS_VERIF);
/*
  for(i=0;i<NRUNS;i++)
  {
    t[i] = cpucycles();
    mul_mn_uniQ_n_uniB(vec_m, vec_n, matrix_At);
  }
  print_results("mul Ay: ", t, NRUNS);

  for(i=0;i<NRUNS;i++)
  {
    t[i] = cpucycles();
    mul_mn_uniQ_n_uniB6x(vec_m, vec_n, matrix_At);
  }
  print_results("6x mul Ay: ", t, NRUNS);

  
  for(i=0;i<NRUNS;i++)
  {
    t[i] = cpucycles();
    mul_mk_gauss_m_sparse_subred(vec_m, sk+sizeof(int64_t)*PARAM_N*PARAM_K, pos, sign);
  }
  print_results("mul Ec subred: ", t, NRUNS);
  */

/*}*/

  fclose(urandom);
  return 0;
}



