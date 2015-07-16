#ifndef _GNU_SOURCE
// libaio, O_DIRECT and other things won't be available without this define
#define _GNU_SOURCE
#endif

//#define DEBUG

//#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <libaio.h>
#include <sys/types.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>


// Used to measure intervals and absolute times
typedef int64_t msec_t;

msec_t time_ms(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (msec_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

void internalWrite(int fd, long position, int size, void * buffer)
{
    if (lseek (fd, position, SEEK_SET) < 0)
    {
         fprintf (stderr, "Error on fseek\n");
         exit(-1);
    }

    if (write(fd, buffer, (size_t)size)<0)
    {
         fprintf (stderr, "Error on write %s\n", strerror(errno));
         exit(-1);
    }

}

char * newBuffer(int blockSize, int size, char fillChar)
{
    fprintf (stdout, "allocating blockSize = %d, size=%d\n", blockSize, size);
    void * buffer;
    int i;
    int result = posix_memalign(&buffer, (size_t)blockSize, (size_t)size);
    if (result != 0)
    {
       fprintf (stderr, "Can't allocate memory\n");
       exit(-1);
    }
    char * charBuffer = (char *) buffer;
    for (i = 0; i < size; i++)
    {
         charBuffer[i] = fillChar;
    }
    return charBuffer;
}

void preAlloc(int fd, int blockSize, int blocks, int type)
{
    fprintf (stderr, "preAlloc blockSize = %d, blocks = %d\n", blockSize, blocks);
    if (type == 0)
    {
       fprintf (stderr, "using fallocate..");
       if (fallocate(fd, 0, 0, (off_t) (blocks * blockSize)))
       {
           fprintf (stderr, "Can't use fallocate\n");
           exit(-1);
       }
       fprintf (stderr, "...done\n");
    }
    else if (type == 1)
    {
       fprintf (stdout, "Writing to allocate with a single big chunk only ...");
        // it will write a single big block 
        char * charBuffer = newBuffer(blockSize, blockSize * blocks, 'a');
        internalWrite(fd, 0, blocks * blockSize, charBuffer);

        if (fsync(fd) < 0)
        {
            fprintf (stderr, "Could not fsync\n");
            exit(-1);
        }

        lseek (fd, 0, SEEK_SET);

        free(charBuffer);
       fprintf (stdout, "...done\n");
    }
    else if (type == 2)
    {
        // it will write multiple small blocks
       fprintf (stdout, "Writing to allocate with %d small chunks of 512 bytes ...", blocks);
        char * charBuffer = newBuffer(blockSize, blockSize, 'a');
        int i;
        for (i = 0; i < blocks; i++)
        {
           internalWrite(fd, i * blockSize, blockSize, charBuffer);
        }

        if (fsync(fd) < 0)
        {
            fprintf (stderr, "Could not fsync\n");
            exit(-1);
        }

        lseek (fd, 0, SEEK_SET);

        free(charBuffer);
       fprintf (stdout, "...done\n");
    }
}

void loopMethod(int fd, int blockSize, int maxIO, int blocks)
{
    io_context_t libaioContext;
    int res = io_queue_init(maxIO, &libaioContext);
    if (res)
    {
        fprintf (stderr, "Can't init queue\n");
        exit(-1);
    }

    int i;

    fprintf (stderr, "writing...");
    char * buffer = newBuffer(blockSize, blockSize, 'd');
    msec_t start = time_ms();
    clock_t startClock = clock();
    for (i = 0; i < blocks; i++)
    {
        struct iocb * iocb = (struct iocb *)malloc(sizeof(struct iocb));
        io_prep_pwrite(iocb, fd, buffer, (size_t)blockSize, i * blockSize);
        int result = io_submit(libaioContext, 1, &iocb);
        if (result < 0)
        {
           fprintf (stderr, "Can't submit\n");
           exit(-1);
        }
    }

    struct io_event * events = (struct io_event *)malloc(sizeof(struct io_event) * (size_t)blocks);

    res = io_getevents(libaioContext, blocks, blocks, events, 0);

    msec_t end = time_ms();
    clock_t endClock = clock();

    for (i = 0; i < res; i++)
    {
       free (events[i].obj);
    }


    free(events);

    msec_t milliseconds = (end - start);
    clock_t clocks = (endClock - startClock);

    fprintf (stderr, "...done in %ld clocks, %ld milliseconds\n", (long)clocks, (long)milliseconds);
}

int main(int argc, char *argv[])
{
    if (argc != 5)
    {
        fprintf (stderr, "usage: libtest blocksize file1 file2 file3\n");
        exit(-1);
    }

    int blockSize = atoi(argv[1]);
    fprintf (stdout, "using blockSize %d\n", blockSize);

    int NUMBER_OF_BLOCKS = 10000;
    int QUEUE_SIZE = 20000;


    int i;

    for (i = 0; i < 3; i++)
    {
       fprintf (stderr, "=================================== test %d\n", i);
       char * file = argv[i + 2];
       fprintf (stderr, "Opening file %s for test %d\n", file, i);
       int fd = open(file, O_RDWR | O_CREAT | O_DIRECT, 0666);
       if (fd < 0)
       {
          fprintf (stderr, "Could not open file %s", argv[1]);
          exit(-1);
       }
       preAlloc(fd, blockSize, NUMBER_OF_BLOCKS, i);
       loopMethod(fd, blockSize, QUEUE_SIZE, NUMBER_OF_BLOCKS);
       close(fd);
    }

    return 0;
}
