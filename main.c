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

int page_size;
// long long memsize;
int block_count;
int thread_count;
int block_size;
char *filename;
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

// void merge(int arg)
// {
//     // printf("merge\n");
//     int step = 1;
//     while (step <= block_count)
//     {
//         int number = arg;
//         step *= 2;
//         while (number >= 0)
//         {
//             number = merger_blocks(step);
//             if (number == -1)
//                 break;

//             struct index_s *temp = malloc(step * block_size * sizeof(struct index_s));
//             memcpy(temp, &(begin[number * block_size]), step * block_size * sizeof(struct index_s));

//             int i = 0, j = step * block_size / 2, k = 0;
//             while (i < step * block_size / 2 && j < step * block_size)
//             {
//                 if (cmp(&(temp[i * block_size]), &(temp[j * block_size])) == 1)
//                     begin[k++ + step * block_size * number] = temp[j++];
//                 else
//                     begin[k++ + step * block_size * number] = temp[i++];
//             }
//             while (j < step * block_size)
//                 begin[k++ + step * block_size * number] = temp[j++];
//             while (i < step * block_size / 2)
//                 begin[k++ + step * block_size * number] = temp[i++];
//             free(temp);
//         }
//         pthread_barrier_wait(&barrier);
//         for (int i = 0; i < block_count; i++)
//             block_status[i] = 1;
//     }
// }

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
    // printf("sort\n");
    pthread_barrier_wait(&barrier);
    // printf("sort bef barr\n");
    while (number >= 0)
    {
        struct index_s *temp = &(begin[number * block_size]);
        qsort(temp, block_size, sizeof(struct index_s), cmp);
        // printf("qsort num %d\t in %ld\n", number, temp->recno);
        // qsort(temp, block_size, sizeof(struct index_s), cmp);
        // printf("qsort ex num %d\t in %ld\n", number, temp->recno);
        number = next_block();
        if (number == -1)
            break;
    }
    pthread_barrier_wait(&barrier);
    // merge(argument);
}

int main(int argc, char *argv[])
{
    if (argc != 4)
    {
        printf("Error parametrs\n");
        return 1;
    }
    // page_size = getpagesize();
    // memsize = atoll(argv[1]);
    records_count = atoi(argv[1]);
    if (records_count % 256 != 0)
    {
        printf("Error records_count\n");
        return 1;
    }
    block_count = atoi(argv[2]);
    if (block_count > 256 || !(block_count > 0 && (block_count & (block_count - 1)) == 0))
    {
        printf("Error blocks\n");
        return 1;
    }
    thread_count = atoi(argv[3]);
    if (thread_count < 12 || thread_count > 32 || block_count < thread_count)
    {
        printf("Error threads\n");
        return 1;
    }

    pthread_mutex_init(&mutex, NULL);
    pthread_barrier_init(&barrier, NULL, thread_count);
    block_size = records_count / block_count;

    // int fd = open("file", O_RDWR);
    // if (fd < 0)
    //     printf("Error open file");
    FILE *file = fopen("file", "rb+");
    if (!file)
        printf("Error open file");

    struct stat st;
    if (stat("file", &st) < 0)
        return -1;
    int fd = fileno(file);
    // printf("%ld\t%d\t%ld\n", records_count, block_size, st.st_size - sizeof(uint64_t));

    // records_count * sizeof(struct index_s) + sizeof(uint64_t)
    begin = (index_record *)((uint8_t *)mmap(
                                 NULL,
                                 st.st_size,
                                 PROT_READ | PROT_WRITE,
                                 MAP_SHARED,
                                 fd,
                                 0) +
                             sizeof(uint64_t));

    // printf("%ld\n", begin->recsords);
    // printf("%.3ld\t%lf\n", begin->recno, begin->time_mark);

    // buffer = malloc(records_count * sizeof(struct index_s));
    // memcpy(buffer,begin, records_count * sizeof(struct index_s));
    // printf("%ld\n", begin->idx[5].recno);
    block_status = (int *)calloc(block_count, sizeof(int));
    // for (int i = 0; i < thread_count; i++)
    //     block_status[i] = 1;
    for (int i = 0; i < block_count; i++)
        block_status[i] = 1;

    int step = 1;
    while (step < block_count)
    {
        int number = 1;
        step *= 2;
        while (number >= 0)
        {
            // printf("merge\n");
            number = merger_blocks(step);
            if (number == -1)
                break;

            printf("num %d step %d mem %d\n", number, step, step * block_size);
            struct index_s *temp = malloc(step * block_size * sizeof(struct index_s));
            memcpy(temp, &(begin[number * block_size * step]), step * block_size * sizeof(struct index_s));
            // printf("merge copy\n");
            // printf("%.3ld\t%lf\n", temp->recno, temp->time_mark);
            // printf("%.3ld\t%lf\n", (temp + step - 1)->recno, (temp + step - 1)->time_mark);
            // printf("=======================================\n");
            int i = 0, j = step * block_size / 2, k = 0;
            // printf("%.3ld\t%lf\n", temp->recno, temp->time_mark);
            // printf("%.3ld\t%lf\n", (temp + j)->recno, (temp + j)->time_mark);
            // printf("%.3ld\t%lf\n", (begin + k)->recno, (begin + k)->time_mark);
            // printf("=======================================\n");
            while (i < step * block_size / 2 && j < step * block_size)
            {
                if (cmp(temp + i, temp + j) == 1)
                // if ((temp + i)->time_mark > (temp + j)->time_mark)
                {
                    begin[k++] = temp[j++];
                    // printf("%.3ld\t%lf\n", (temp + j)->recno, (temp + j)->time_mark);
                }
                else
                {
                    begin[k++] = temp[i++];
                    // printf("%.3ld\t%lf\n", (temp + i)->recno, (temp + i)->time_mark);
                }
                // printf("%.3ld\t%lf\n", (temp + i)->recno, (temp + i)->time_mark);
                // printf("%.3ld\t%lf\n", (temp + j)->recno, (temp + j)->time_mark);
                printf("%.3ld\t%lf\n", (begin + k)->recno, (begin + k)->time_mark);
                // i++;
                // j++;

                // if (cmp(&(temp[i * block_size]), &(temp[j * block_size])) == 1)
                //     begin[k++ + step * block_size * number] = temp[j++];
                // else
                //     begin[k++ + step * block_size * number] = temp[i++];
                printf("----------------------------------------\n");
            }
            while (j < step * block_size)
                begin[k++] = temp[j++];
            while (i < step * block_size / 2)
                begin[k++] = temp[i++];
            free(temp);
            // printf("merge\n");
        }
        printf("#########################################\n");
        // pthread_barrier_wait(&barrier);
        for (int i = 0; i < block_count; i++)
            block_status[i] = 1;
    }

    // pthread_t *threads = (pthread_t *)malloc(thread_count * sizeof(pthread_t));
    // int *arg = (int *)malloc(thread_count * sizeof(int));
    // for (int i = 0; i < thread_count; i++)
    // {
    //     arg[i] = i;
    //     pthread_create(&threads[i], NULL, sort, &arg[i]);
    // }
    // free(arg);

    // for (int i = 0; i < thread_count; i++)
    //     pthread_join(threads[i], 0);

    // free(threads);
    free(block_status);
    fclose(file);
    munmap(begin - sizeof(uint64_t), st.st_size);
    pthread_barrier_destroy(&barrier);
    pthread_mutex_destroy(&mutex);
    return 0;
}
