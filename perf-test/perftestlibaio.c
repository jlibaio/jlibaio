#ifndef _GNU_SOURCE
// libaio, O_DIRECT and other things won't be available without this define
#define _GNU_SOURCE
#endif

//#define DEBUG

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

char * newBuffer(int size, char fillChar)
{
    void * buffer;
    int i;
    int result = posix_memalign(&buffer, (size_t)512, (size_t)size);
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

void preAlloc(int fd, int blocks, int useFallocate)
{
    if (useFallocate)
    {
       fprintf (stderr, "using fallocate..");
       if (fallocate(fd, FALLOC_FL_ZERO_RANGE, 0, (off_t) (blocks * 512)))
       {
           fprintf (stderr, "Can't use fallocate\n");
           exit(-1);
       }
       fprintf (stderr, "...done\n");
    }
    else
    {
       fprintf (stderr, "Writing to allocate...");
        char * charBuffer = newBuffer(512 * blocks, 'a');
        internalWrite(fd, 0, blocks * 512, charBuffer);

        if (fsync(fd) < 0)
        {
            fprintf (stderr, "Could not fsync\n");
            exit(-1);
        }

        lseek (fd, 0, SEEK_SET);

        free(charBuffer);
       fprintf (stderr, "...done\n");
    }
}

void loopMethod(int fd, int maxIO, int blocks)
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
    char * buffer = newBuffer(512, 'd');
    clock_t start = clock();
    for (i = 0; i < blocks; i++)
    {
        struct iocb * iocb = (struct iocb *)malloc(sizeof(struct iocb));
        io_prep_pwrite(iocb, fd, buffer, (size_t)512, i * 512);
        int result = io_submit(libaioContext, 1, &iocb);
        if (result < 0)
        {
           fprintf (stderr, "Can't submit\n");
           exit(-1);
        }
    }

    struct io_event * events = (struct io_event *)malloc(sizeof(struct io_event) * (size_t)blocks);

    res = io_getevents(libaioContext, blocks, blocks, events, 0);

    for (i = 0; i < res; i++)
    {
       free (events[i].obj);
    }

    clock_t end = clock();

    free(events);

    long clocks = (end - start);

    fprintf (stderr, "...done in %ld clocks\n", clocks);
}

int main(int argc, char *argv[])
{
    if (argc != 3)
    {
        fprintf (stderr, "usage: libtest regular-file fallocated-file\n");
        exit(-1);
    }

    int NUMBER_OF_BLOCKS = 10000;
    int QUEUE_SIZE = 20000;

    fprintf (stderr, "Opening %s\n", argv[1]);

    int fd = open(argv[1], O_RDWR | O_CREAT | O_DIRECT, 0666);
    if (fd < 0)
    {
       fprintf (stderr, "Could not open file %s", argv[1]);
       exit(-1);
    }

    preAlloc(fd, NUMBER_OF_BLOCKS, 0);
    loopMethod(fd, QUEUE_SIZE, NUMBER_OF_BLOCKS);
    close(fd);

    fprintf (stderr, "Opening %s\n", argv[2]);

    fd = open(argv[2], O_RDWR | O_CREAT | O_DIRECT, 0666);
    if (fd < 0)
    {
       fprintf (stderr, "Could not open file %s", argv[2]);
       exit(-1);
    }

    preAlloc(fd, NUMBER_OF_BLOCKS, 1);
    loopMethod(fd, QUEUE_SIZE, NUMBER_OF_BLOCKS);
    close(fd);


    return 0;
}