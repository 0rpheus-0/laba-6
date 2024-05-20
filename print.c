#include "index.h"
#include <stdio.h>
#include <stdlib.h>

int main()
{
    FILE *file = fopen("file", "rb");
    uint64_t count;

    fread(&count, sizeof(uint64_t), 1, file);
    double temp_1;
    uint64_t temp_2;
    for (int i = 0; i < (int)count; i++)
    {
        fread(&temp_1, sizeof(double), 1, file);
        fread(&temp_2, sizeof(uint64_t), 1, file);
        printf("%.3ld\t%lf\n", temp_2, temp_1);
    }
    return 0;
}