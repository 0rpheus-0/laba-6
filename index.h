#define _GNU_SOURCE
#include <stdint.h>

typedef struct index_s
{
    double time_mark;
    uint64_t recno;
} index_record;

typedef struct index_hdr_s
{
    uint64_t recsords;
    struct index_s *idx;
} data_type;