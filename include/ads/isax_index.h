//
//  isax_index.h
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/7/12.
//  Copyright 2012 University of Trento. All rights reserved.
//

#ifndef isaxlib_isax_index_h
#define isaxlib_isax_index_h
#include <pthread.h>
#include <stdbool.h>

#include "../../config.h"
#include "../../globals.h"
#include "isax_node.h"
#include "isax_node_record.h"
#include "isax_first_buffer_layer.h"
#include "sax/ts.h"
#include "pqueue.h"
typedef struct {
    unsigned long mem_tree_structure;
    unsigned long mem_data;
    unsigned long mem_summaries;
    unsigned long disk_data_full;
    unsigned long disk_data_partial;
} meminfo;


typedef struct {
    char new_index;

    char * raw_filename;
    const char* root_directory;
    int initial_fbl_buffer_size;
	sax_type *max_sax_cardinalities;

    // ALWAYS: TIMESERIES_SIZE = TS_VALUES_PER_PAA_SEGMENT * PAA_SEGMENTS
    int timeseries_size;
    int ts_values_per_paa_segment;
    int paa_segments;

	int tight_bound;
	int aggressive_check;

    int sax_byte_size;
    int position_byte_size;
    int ts_byte_size;

    int full_record_size;
    int partial_record_size;

    // ALWAYS: SAX_ALPHABET_CARDINALITY = 2^SAX_BIT_CARDINALITY
    int sax_bit_cardinality;
    root_mask_type * bit_masks;
    int sax_alphabet_cardinality;

    int max_leaf_size;
    int min_leaf_size;
    int initial_leaf_buffer_size;
    int max_total_buffer_size;
    int max_total_full_buffer_size;

    int max_filename_size;

    float mindist_sqrt;
    int root_nodes_size;

    int total_loaded_leaves;
    //////////////////////////
    int attribute_size;
    int attribute_max_value;
    int attribute_min_value;
    //////////////////////////

} isax_index_settings;

typedef struct isax_index{
    meminfo memory_info;

    FILE *sax_file; //not needed to messi should not measure the time
    sax_type *sax_cache; //not in use
    unsigned long sax_cache_size; // not in use

    unsigned long long allocated_memory;    //counts who many memory in system -always enough memory
    unsigned long root_nodes;   // -who many root children we have-
    unsigned long long total_records;   //total number of timeseries to index (how many series u will process)
    unsigned long long loaded_records; //not needed for Messi

    int * locations;    //not needed for Messi
    struct isax_node *first_node; //not needed for Messi
    isax_index_settings *settings; //not needed for Messi

    char has_wedges;    //not needed for Messi

    struct first_buffer_layer *fbl; //pointer to the rec buffers - first buffer layer (ISAX buffer in papper)

    ts_type *answer;    //not needed for Messi
} isax_index;



//TODO: Put sanity check for variables (cardinalities etc.)

isax_index * isax_index_init(isax_index_settings *settings);
isax_index_settings * isax_index_settings_init (const char * root_directory,
                                                int timeseries_size,
                                                int paa_segments,
                                                int sax_bit_cardinality,
                                                int max_leaf_size,
                                                int min_leaf_size,
                                                int initial_leaf_buffer_size,
                                                int max_total_buffer_size,
                                                int initial_fbl_buffer_size,
                                                int total_loaded_leaves,
												int tight_bound, int aggressive_check, int new_index ,char inmemory_flag,int attribute_size,int attribute_max_value,int attribute_min_value);
void print_settings(isax_index_settings *settings);

isax_node * add_record_to_node(isax_index *index, isax_node *node,
                                 isax_node_record *record,
                                 const char leaf_size_check);
isax_node * add_record_to_node_inmemory(isax_index *index,
                                 isax_node *tree_node,
                                 isax_node_record *record,
                                 const char leaf_size_check);
isax_node * add_record_to_node_inmemory_parallel_locks(isax_index *index,
                                 isax_node *tree_node,
                                 isax_node_record *record);
typedef struct parallel_fbl_soft_buffer_ekosmas_lf parallel_fbl_soft_buffer_ekosmas_lf;
isax_node * add_record_to_node_inmemory_parallel_lockfree_announce(isax_index *index,
                                 parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node,
                                 isax_node_record *record,
                                 unsigned long my_id,
                                 unsigned long total_workers_num,
                                 const char is_helper,
                                 const char lockfree_parallelism_in_subtree);
isax_node * add_record_to_node_inmemory_parallel_lockfree_announce_local(isax_index *index,
                                 isax_node *tree_node,
                                 parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node,
                                 isax_node_record *record,
                                 unsigned long total_workers_num,
                                 unsigned long my_id,
                                 const char lockfree_parallelism_in_subtree,
                                 unsigned char lightweight_path);
void add_record_to_node_inmemory_parallel_lockfree_cow_local(isax_index *index,
                                 isax_node *tree_node,
                                 isax_node_record *record,
                                 unsigned long my_id);
void add_record_to_node_inmemory_parallel_lockfree_cow_local_geopat(isax_index *index,
                                 isax_node_single_buffer *tree_node,
                                 isax_node_record *record,
                                 unsigned long my_id,int function_type) ;                               
isax_node * add_record_to_node_inmemory_parallel_lockfree_cow(isax_index *index,
                                 parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node,
                                 isax_node_record *record,
                                 unsigned long my_id);
isax_node * add_record_to_node_inmemory_parallel_lockfree_cow_geopat(isax_index *index,
                                 parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,
                                 isax_node_record *record,
                                 unsigned long my_id,root_mask_type mask,unsigned long current_pos); 
isax_node_single_buffer *  add_record_to_node_inmemory_parallel_lockfree_geopat(isax_index *index,
                                 parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,
                                 isax_node_record *record,
                                 unsigned long my_id,root_mask_type mask,unsigned long current_pos,int rec_buff_exists);                                                                
enum response create_node_filename(isax_index *index,
                                   isax_node *node,
                                   isax_node_record *record);

isax_index * isax_index_init_inmemory(isax_index_settings *settings);
isax_index * isax_index_init_inmemory_ekosmas(isax_index_settings *settings);
unsigned long populate_tree_lock_free_cow_geopat_flag(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node, isax_node_record *r, unsigned long my_id, int rec_buff_exists,unsigned long current_pos);
int insertIsaxToTree(isax_index *index,parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,unsigned long my_id,InfoRecord * op,int rec_buff_exists,int helper);


void isax_index_destroy(isax_index *index, isax_node *node);
void isax_index_pRecBuf_destroy(isax_index *index, isax_node *node,int prewokernumber);
void isax_tree_destroy(isax_node *node);                                                    // EKOSMAS: ADDED JUNE 16 2020
void isax_tree_destroy_lockfree(isax_node *node);                                           // EKOSMAS: AUGUST 08 2020
void isax_tree_destroy_lockfree_geopat(isax_node_single_buffer *node);
int comp(const void * a, const void * b);

#endif
