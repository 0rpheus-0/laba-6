#define _GNU_SOURCE
#include <stdint.h>

typedef struct index_s
{
    double time_mark; // временная метка (модифицированная юлианская дата)
    uint64_t recno;   // первичный индекс в таблице БД
} index_record;

typedef struct index_hdr_s
{
    uint64_t recsords;   // количество записей
    struct index_s *idx; // массив записей в количестве records
} data_type;