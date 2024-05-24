#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>

int block_count;
int thread_count;
int block_size;
uint64_t records_count;

pthread_barrier_t barrier;
pthread_mutex_t mutex;
pthread_mutex_t number_mutex;
struct index_hdr_s *buffer;
int start = 0;
int *block_status;
index_record *begin;
uint64_t count = 0;

int cmp(const void *a, const void *b)
{
    struct index_s num1 = *((struct index_s *)a);
    struct index_s num2 = *((struct index_s *)b);
    if (num1.time_mark < num2.time_mark)
        return -1;
    else if (num1.time_mark > num2.time_mark)
        return 1;
    else
        return 0;
}

int merger_blocks(int step)
{
    int number = -1;
    pthread_mutex_lock(&mutex);
    for (int i = 0; i < block_count / step; i++)
        if (block_status[i])
        {
            block_status[i] = 0;
            number = i;
            break;
        }
    pthread_mutex_unlock(&mutex);
    return number;
}

void merge(int arg)
{
    int step = 1;
    while (step <= block_count)
    {
        int number = 1;
        step *= 2;
        while (number >= 0)
        {
            number = merger_blocks(step);
            if (number == -1)
                break;
            struct index_s *temp = malloc(step * block_size * sizeof(struct index_s));
            memcpy(temp, &(begin[number * block_size * step]), step * block_size * sizeof(struct index_s));
            int i = 0, j = step * block_size / 2, k = step * number * block_size;
            while (i < step * block_size / 2 && j < step * block_size)
            {
                if (cmp(temp + i, temp + j) == 1)
                    begin[k++] = temp[j++];
                else
                    begin[k++] = temp[i++];
            }
            while (j < step * block_size)
                begin[k++] = temp[j++];
            while (i < step * block_size / 2)
                begin[k++] = temp[i++];
            free(temp);
        }
        pthread_barrier_wait(&barrier);
        if (arg == 0)
            for (int i = 0; i < block_count; i++)
                block_status[i] = 1;
        pthread_barrier_wait(&barrier);
    }
}

int next_block()
{
    int number = -1;
    pthread_mutex_lock(&mutex);
    for (int i = 0; i < block_count; i++)
        if (!block_status[i])
        {
            block_status[i] = 1;
            number = i;
            break;
        }
    pthread_mutex_unlock(&mutex);
    return number;
}

void *sort(void *arg)
{
    int argument = *(int *)arg;
    int number = argument;
    pthread_barrier_wait(&barrier);
    while (number >= 0)
    {
        struct index_s *temp = &(begin[number * block_size]);
        qsort(temp, block_size, sizeof(struct index_s), cmp);
        number = next_block();
        if (number == -1)
            break;
    }
    pthread_barrier_wait(&barrier);
    merge(argument);
    return NULL;
}

int main(int argc, char *argv[])
{
    if (argc != 3)
    {
        printf("Error parametrs\n");
        return 1;
    }
    block_count = atoi(argv[1]);
    if (block_count > 256 || !(block_count > 0 && (block_count & (block_count - 1)) == 0))
    {
        printf("Error blocks\n");
        return 1;
    }
    thread_count = atoi(argv[2]);
    if (thread_count < 12 || thread_count > 32 || block_count < thread_count)
    {
        printf("Error threads\n");
        return 1;
    }

    pthread_mutex_init(&mutex, NULL);
    pthread_barrier_init(&barrier, NULL, thread_count);

    FILE *file = fopen("file", "rb+");
    if (!file)
        printf("Error open file");

    fread(&records_count, sizeof(uint64_t), 1, file);
    block_size = records_count / block_count;

    struct stat st;
    if (stat("file", &st) < 0)
        return -1;
    int fd = fileno(file);
    begin = (index_record *)((uint8_t *)mmap(
                                 NULL,
                                 st.st_size,
                                 PROT_READ | PROT_WRITE,
                                 MAP_SHARED,
                                 fd,
                                 0) +
                             sizeof(uint64_t));
    block_status = (int *)calloc(block_count, sizeof(int));
    for (int i = 0; i < thread_count; i++)
        block_status[i] = 1;

    pthread_t *threads = (pthread_t *)malloc(thread_count * sizeof(pthread_t));
    int *arg = (int *)malloc(thread_count * sizeof(int));
    for (int i = 0; i < thread_count; i++)
    {
        arg[i] = i;
        pthread_create(&threads[i], NULL, sort, &arg[i]);
    }
    free(arg);

    for (int i = 0; i < thread_count; i++)
        pthread_join(threads[i], 0);

    free(threads);
    free(block_status);
    fclose(file);
    munmap(begin - sizeof(uint64_t), st.st_size);
    pthread_barrier_destroy(&barrier);
    pthread_mutex_destroy(&mutex);
    return 0;
}
