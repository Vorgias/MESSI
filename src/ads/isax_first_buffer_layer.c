//
//  first_buffer_layer.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/20/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <math.h>
#include "ads/sax/sax.h"
#include "ads/isax_first_buffer_layer.h"

struct first_buffer_layer * initialize_fbl(int initial_buffer_size, int number_of_buffers,
                                           int max_total_buffers_size, isax_index *index)
{
    struct first_buffer_layer *fbl = malloc(sizeof(first_buffer_layer));

    fbl->max_total_size = max_total_buffers_size;
    fbl->initial_buffer_size = initial_buffer_size;
    fbl->number_of_buffers = number_of_buffers;

    // Allocate a big chunk of memory to store sax data and positions
    long long hard_buffer_size = (long long)(index->settings->sax_byte_size + index->settings->position_byte_size) * (long long) max_total_buffers_size;
    fbl->hard_buffer = malloc(hard_buffer_size);

    if(fbl->hard_buffer == NULL) {
	fprintf(stderr, "Could not initialize hard buffer of size: %lld\n", hard_buffer_size);
	exit(-1);
    }

    // Allocate a set of soft buffers to hold pointers to the hard buffer
    fbl->soft_buffers = malloc(sizeof(fbl_soft_buffer) * number_of_buffers);
    fbl->current_record_index = 0;
    fbl->current_record = fbl->hard_buffer;
    int i;
    for (i=0; i<number_of_buffers; i++) {
        fbl->soft_buffers[i].initialized = 0;
    }
    return fbl;
}

// Botao's version
struct parallel_first_buffer_layer * initialize_pRecBuf(int initial_buffer_size, int number_of_buffers,
                                           int max_total_buffers_size, isax_index *index) 
{
    struct parallel_first_buffer_layer *fbl = malloc(sizeof(parallel_first_buffer_layer));
    
    fbl->max_total_size = max_total_buffers_size;
    fbl->initial_buffer_size = initial_buffer_size;
    fbl->number_of_buffers = number_of_buffers;
    
    // Allocate a big chunk of memory to store sax data and positions
    long long hard_buffer_size = (long long)(index->settings->sax_byte_size + index->settings->position_byte_size) * (long long) max_total_buffers_size;
    //fbl->hard_buffer = malloc(hard_buffer_size);
           
    // Allocate a set of soft buffers to hold pointers to the hard buffer
    fbl->soft_buffers = malloc(sizeof(parallel_fbl_soft_buffer) * number_of_buffers);
    fbl->current_record_index = 0;
    int i;
    for (i=0; i<number_of_buffers; i++) {
        fbl->soft_buffers[i].initialized = 0;
        fbl->soft_buffers[i].finished = 0;
    }
    return fbl;
}

// ekosmas version
struct parallel_first_buffer_layer_ekosmas * initialize_pRecBuf_ekosmas(int initial_buffer_size, int number_of_buffers,
                                           int max_total_buffers_size, isax_index *index) 
{
    struct parallel_first_buffer_layer_ekosmas *fbl = malloc(sizeof(parallel_first_buffer_layer_ekosmas));
    
    fbl->max_total_size = max_total_buffers_size;
    fbl->initial_buffer_size = initial_buffer_size;
    fbl->number_of_buffers = number_of_buffers;
    
    // Allocate a big chunk of memory to store sax data and positions
    // long long hard_buffer_size = (long long)(index->settings->sax_byte_size + index->settings->position_byte_size) * (long long) max_total_buffers_size;
    //fbl->hard_buffer = malloc(hard_buffer_size);
           
    // Allocate a set of soft buffers to hold pointers to the hard buffer
    fbl->soft_buffers = malloc(sizeof(parallel_fbl_soft_buffer_ekosmas) * number_of_buffers);
    fbl->current_record_index = 0;
    int i;
    for (i=0; i<number_of_buffers; i++) {
        fbl->soft_buffers[i].initialized = 0;
        fbl->soft_buffers[i].finished = 0;
        fbl->soft_buffers[i].node = NULL;                   // EKOSMAS: ADDED 02 NOVEMBER 2020
    }

    return fbl;
}

// ekosmas lock free version
struct parallel_first_buffer_layer_ekosmas_lf * initialize_pRecBuf_ekosmas_lf(int initial_buffer_size, int number_of_buffers,
                                           int max_total_buffers_size, isax_index *index)
{
    struct parallel_first_buffer_layer_ekosmas_lf *fbl = malloc(sizeof(parallel_first_buffer_layer_ekosmas_lf));

    fbl->max_total_size = max_total_buffers_size;
    fbl->initial_buffer_size = initial_buffer_size;
    fbl->number_of_buffers = number_of_buffers;
    
    // Allocate a set of soft buffers to hold pointers to the hard buffer
    fbl->soft_buffers = malloc(sizeof(parallel_fbl_soft_buffer_ekosmas_lf) * number_of_buffers);
    fbl->current_record_index = 0;
    int i;
    for (i=0; i<number_of_buffers; i++) {
        fbl->soft_buffers[i].initialized = 0;
        fbl->soft_buffers[i].processed = 0;
        fbl->soft_buffers[i].next_iSAX_group = 0;           // EKOSMAS: ADDED 07 JULY 2020
        fbl->soft_buffers[i].max_buffer_size = NULL;        // EKOSMAS: ADDED 30 JUNE 2020
        fbl->soft_buffers[i].buffer_size = NULL;            // EKOSMAS: ADDED 30 JUNE 2020
        fbl->soft_buffers[i].sax_records = NULL;            // EKOSMAS: ADDED 30 JUNE 2020
        fbl->soft_buffers[i].pos_records = NULL;            // EKOSMAS: ADDED 30 JUNE 2020
        fbl->soft_buffers[i].node = NULL;                   // EKOSMAS: ADDED 30 JUNE 2020
        fbl->soft_buffers[i].iSAX_processed = NULL;         // EKOSMAS: ADDED 20 JULY 2020
        // fbl->soft_buffers[i].announce_array = NULL;         // EKOSMAS: ADDED 29 JULY 2020
        fbl->soft_buffers[i].recBuf_helpers_exist = 0;      // EKOSMAS: ADDED 07 AUGUST 2020
    }
    return fbl;
}

// ekosmas lock free version
struct parallel_first_buffer_layer_ekosmas_lf_geopat * initialize_pRecBuf_ekosmas_lf_geopat(int initial_buffer_size, int number_of_buffers,
                                           int max_total_buffers_size, isax_index *index)
{
    struct parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = malloc(sizeof(parallel_first_buffer_layer_ekosmas_lf_geopat));




    fbl->max_total_size = max_total_buffers_size;
    fbl->initial_buffer_size = initial_buffer_size;
    fbl->number_of_buffers = number_of_buffers;
    
    // Allocate a set of soft buffers to hold pointers to the hard buffer
    fbl->soft_buffers = malloc(sizeof(parallel_fbl_soft_buffer_ekosmas_lf_geopat) * number_of_buffers);
    fbl->current_record_index = 0;
    int i;
    int k = 0;
    int number_of_subtrees = (int)pow(2, index->settings->paa_segments);
    long int j = 0;
    int finish_receive_buffer = 0;
    parallel_fbl_soft_buffer_ekosmas_lf_geopat* tmp = malloc(sizeof(parallel_fbl_soft_buffer_ekosmas_lf_geopat) * number_of_buffers);
    for (i=0; i<number_of_buffers; i++) {
        tmp[i].initialized = 0;
        tmp[i].processed = 0;
        tmp[i].max_buffer_size = NULL;        // EKOSMAS: ADDED 30 JUNE 2020
        tmp[i].buffer_size = NULL;            // EKOSMAS: ADDED 30 JUNE 2020
        tmp[i].sax_records = malloc(sizeof(sax_type*)*initial_buffer_size);
        tmp[i].pos_records = malloc(sizeof(file_position_type*)*initial_buffer_size);
        tmp[i].masks = malloc(sizeof(root_mask_type) *initial_buffer_size);
        tmp[i].node = malloc(sizeof(isax_node_single_buffer*) * number_of_subtrees);
        tmp[i].rootSubtreesUpdateArray = malloc(sizeof(Update*) * number_of_subtrees);
        printf("Subtrees = %d\n", number_of_subtrees);
        for(k=0;k<number_of_subtrees;k++){
             tmp[i].node[k] = NULL;
             tmp[i].rootSubtreesUpdateArray[k] = malloc(sizeof(Update));
             tmp[i].rootSubtreesUpdateArray[k]->state = CLEAN;
             tmp[i].rootSubtreesUpdateArray[k]->info = NULL;
        }
         tmp[i].finish_receive_buffer = malloc(sizeof(int));
        *tmp[i].finish_receive_buffer = 0;
         tmp[i].iSAX_processed = malloc(sizeof(unsigned long) * initial_buffer_size);

        for(unsigned long j =0;j<initial_buffer_size;j++){
            tmp[i].sax_records[j] = malloc(sizeof(sax_type) * index->settings->paa_segments);
            tmp[i].pos_records[j] = malloc(sizeof(file_position_type));
            tmp[i].masks[j] = 0;
            tmp[i].iSAX_processed[j] = 0;
        }
    }
    fbl->soft_buffers = tmp;
    return fbl;
}


void destroy_fbl(first_buffer_layer *fbl) {

    free(fbl->hard_buffer);
    free(fbl->soft_buffers);
    free(fbl);
}
void destroy_pRecBuf(parallel_first_buffer_layer *fbl,int prewokernumber) {

    for (int j=0; j<fbl->number_of_buffers; j++)
    {
         parallel_fbl_soft_buffer *current_fbl_node = &fbl->soft_buffers[j];
        if (!current_fbl_node->initialized) {
            continue;
        }
        for (int k = 0; k < prewokernumber; k++)
        {
            if(current_fbl_node->sax_records[k]!=NULL)
            {
                free((current_fbl_node->sax_records[k]));
                free((current_fbl_node->pos_records[k]));
                current_fbl_node->sax_records[k]=NULL;
                current_fbl_node->pos_records[k]=NULL;
            }
        }
        free(current_fbl_node->buffer_size);
        free(current_fbl_node->max_buffer_size);
        free(current_fbl_node->sax_records);
        free(current_fbl_node->pos_records);
    }
    free(fbl->soft_buffers);
    free(fbl);
}
