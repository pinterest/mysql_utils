#include <stdio.h>
#include <stdlib.h>

/*
This program has been created to handle an incompatibility between how mysqldump escapes
some characters and how hive interprets those escaped chars. It does the following:

If you see an 0x5c30 in the input sequence

a. and there is no or even number of 0x5c before 0x5c30, translate this 0x5c30 to 0x00

b. if there is odd number of 0x5c before 0x5c30, don't do anything.

Some sample transforms:

  0x5c30 => 0x00

  0x5c5c30 => 0x5c5c30

  0x5c5c5c30 => 0x5c5c00

  0x5c5c5c5c30 => 0x5c5c5c5c30

  0x5c5c5c3030 => 0x5c5c0030

  0x5c5c5c5c3030 => 0x5c5c5c5c3030

  0x5c5c5c40 => 0x5c5c5c40

  0x5c5c5c5c40 => 0x5c5c5c5c40

Here is another way to test:

- Create table with blob content:  create table MyTest (id integer, value1 varchar(20), content blob, value2 double, primary key(id));

- Insert into blob content:  insert into MyTest (id, value1, content, value2) values (1, "data1", 0x3020090d0a2227005c30, 2.2);

- checking content: select hex(content) from MyTest;

- chmod a+rw /tmp/dump

- mysqldump -u root --tab=/tmp/dump --single-transaction -- create-options test

- see content:  hexdump /tmp/dump/MyTest.txt

hexdump of original dump file:

0000000 31 09 64 61 74 61 31 09 30 20 5c 09 0d 5c 0a 22

0000010 27 5c 30 5c 5c 30 09 32 2e 32 0a               

000001b


hexdump after passing through this program:

0000000 31 09 64 61 74 61 31 09 30 20 5c 09 0d 5c 0a 22

0000010 27 00 5c 5c 30 09 32 2e 32 0a                  

000001a

Author : vamsi Nov 2015

*/

#define FALSE 0
#define TRUE 1

#define RBUFFERLEN 65536

char bufferR[RBUFFERLEN];
int ibufferR = 0; /* index in bufferR */
int nbufferR = 0; /* number of valid chars in bufferR */


#define WBUFFERLEN 65536

char bufferW[WBUFFERLEN];
int ibufferW = 0; /* index in bufferW upto which we wrote */

/* wrapper for efficieny. Returns if eof reached. If not, puts next char in *addrc  */
int getcharWrapper(char *addrc) {
  int error = 0;

  if ((nbufferR <= ibufferR) && !feof(stdin))  {
    /* we used up what we read earlier */

    nbufferR = fread(bufferR, 1, RBUFFERLEN, stdin);
    ibufferR = 0;

    if ((nbufferR != RBUFFERLEN) && !feof(stdin)) {
      error = ferror(stdin);
      fprintf(stderr, "Read failed with error %d\n", error);
      exit(1);
    }
  }

  if (nbufferR <= ibufferR) {
    return TRUE;
  }

  *addrc = bufferR[ibufferR];
  ibufferR++;
  return FALSE;
}

void flush() {
  int error = 0;
  int written = 0;
  written = fwrite(bufferW, 1, ibufferW, stdout);

  if (written != ibufferW) {
    error = ferror(stdout);
    fprintf(stderr, "Write failed with error %d\n", error);
    exit(2);
  }

  ibufferW = 0;
}

/* wrapper to buffer */
void putcharWrapper(char c) {
  int error = 0;
  int written = 0;

  if (ibufferW >= WBUFFERLEN) {
    /* buffer full */
    flush();
  }

  if (ibufferW >= WBUFFERLEN) {
    fprintf(stderr, "Buffer full even after flush %d\n", error);
    exit(3);
  }

  bufferW[ibufferW] = c;
  ibufferW++;
}

int main(int argc, char** argv) {
  char c;
  int eof = FALSE;
  while (!(eof = getcharWrapper(&c))) {
    if (c != 0x5c) {
      putcharWrapper(c);
      continue;
    }

    /*
     * if we reach here, we have one outstanding 0x5c that we have not yet put in output.
     * let us count consecutive 0x5c that we see excluding the outstanding one.
     */
    long count = 0;

    /* we found 0x5c. Keep reading until we get EOF or something other than 0x5c */
    while (!(eof = getcharWrapper(&c))) {
      if (c == 0x30) {
        if (count % 2 == 0) {
          /* we saw 0 or even number of 0x5c before 0x5c30 */
          putcharWrapper(0x00);
          break;
	} else {
          /*
           * we saw odd number of 0x5c before 0x5c30. put the outstanding 0c5c in the output,
           * and then 0x30
           */
          putcharWrapper(0x5c);
          putcharWrapper(0x30);
          break;
	}
      } else if (c == 0x5c) {
        putcharWrapper(0x5c);
        count++;
      } else {
        /* put the outstanding 0x5c and the char we just read in output */
        putcharWrapper(0x5c);
        putcharWrapper(c);
        break;
      }
    }

    if (eof) {
      /* put the outstanding 0x5c */
      putcharWrapper(0x5c);
      flush();
      fflush(stdout);
      return 0;
    }

    /* if we reach here we should not have any outstanding 0x5c. just continbue reading. */
  }

  flush();
  fflush(stdout);
}
