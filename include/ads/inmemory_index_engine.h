//
//  Updated by Eleftherios Kosmas on May 2020.
//

#ifndef al_inmemory_index_engine_h
#define al_inmemory_index_engine_h



#include "../../config.h"
#include "../../globals.h"
#include "sax/ts.h"
#include "sax/sax.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "isax_index.h"
#include "isax_query_engine.h"
#include "parallel_query_engine.h"
#include "isax_node.h"
#include "pqueue.h"
#include "isax_first_buffer_layer.h"
#include "ads/isax_node_split.h"

void backoff_delay(unsigned long backoff);
///////////////////////////////////////////////////
 void tree_index_creation_from_pRecBuf_fai_blocking(void *transferdata);
//////////////////////////////////////////////////
void index_creation_pRecBuf_new(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas(const char *ifilename, long int ts_num, isax_index *index,const char*afilename);//////////////////////////////////////
void index_creation_pRecBuf_new_ekosmas_MESSI_with_enhanced_blocking_parallelism(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_EP(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_blocking_parallelism_in_subtree(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce_after_help(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_cow(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_geopat_ep(const char *ifilename, long int ts_num, isax_index *index , int subtree_parallelism);
void index_creation_pRecBuf_geopat_fetch_and_add_phase1_backoff(const char *ifilename, long int ts_num, isax_index *index ,int subtree_parallelism,int fai_step,int back_off);
void index_creation_pRecBuf_geopat_fetch_and_add_phase1_backoff_microbenchmark(const char *ifilename, long int ts_num, isax_index *index ,int subtree_parallelism,int fai_step);
void index_creation_pRecBuf_geopat_fetch_and_add_phase1_caches_after_Read_file(const char *ifilename, long int ts_num, isax_index *index ,int subtree_parallelism,int fai_step);
void index_initialization_rawBuffer_geopat (const char *ifilename, long int ts_num, isax_index *index , int subtree_parallelism);
void index_creation_pRecBuf_geopat_fetch_and_add_phase1(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism,int fai_step);
void index_creation_pRecBuf_geopat_cyclic(const char *ifilename, long int ts_num, isax_index *index ,int subtree_parallelism,int fai_step);
void index_creation_pRecBuf_geopat_doall(const char *ifilename, long int ts_num, isax_index *index ,int subtree_parallelism,int fai_step);
void index_creation_pRecBuf_geopat_FULL_CAS(const char *ifilename, long int ts_num, isax_index *index ,int subtree_parallelism,int fai_step);
void index_creation_pRecBuf_geopat_ep_skip_chunks(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism);
void index_creation_pRecBuf_geopat_ep_skip_chunks_opt(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism);
void index_creation_pRecBuf_geopat_chunk_size(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism,int chunk_size);
void index_creation_No_pRecBuf_geopat(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism);
void index_creation_No_pRecBuf_geopat_1chunk_simple_help(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism);
void index_creation_No_pRecBuf_geopat_do_all_opt(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism);
void index_creation_No_pRecBuf_geopat_chunk_size(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism,int chunk_size);
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_blocking_parallelism_in_subtree(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_cow(const char *ifilename, long int ts_num, isax_index *index);
void* index_creation_pRecBuf_worker_new(void *transferdata);
void* index_creation_pRecBuf_worker_new_ekosmas(void *transferdata);
void *index_creation_single_pRecBuf_worker_cyclic(void *transferdata);
void *index_creation_single_pRecBuf_worker_geopat_doall(void *transferdata);
void *index_creation_single_pRecBuf_worker_geopat2_FULL_CAS(void *transferdata);
void *index_creation_single_pRecBuf_worker_geopat2_FULL_FAI(void *transferdata);
void* index_creation_single_pRecBuf_worker_geopat_ep(void *transferdata);
void *index_creation_single_pRecBuf_worker_geopat_ep_skip_chunks(void *transferdata);
void *index_creation_single_pRecBuf_worker_geopat_ep_skip_chunks_opt(void *transferdata);
void *index_creation_single_pRecBuf_worker_geopat_chunks(void *transferdata);
void *index_creation_NO_pRecBuf_worker_geopat(void *transferdata);
void *index_creation_NO_pRecBuf_worker_geopat_Do_ALL_OPT(void *transferdata);
void *index_creation_NO_pRecBuf_worker_geopat_chunks_simple_help(void *transferdata);
void *index_creation_NO_pRecBuf_worker_geopat_chunks(void *transferdata);
void* index_creation_pRecBuf_worker_new_ekosmas_EP(void *transferdata);
void* index_creation_pRecBuf_worker_new_ekosmas_lock_free_full_fai(void *transferdata);
void* index_creation_pRecBuf_worker_new_ekosmas_lock_free_fai_only_after_help(void *transferdata);
void check_validity_lf_geopat(isax_index *index, long int ts_num,int parallelism_in_subtree);
root_mask_type isax_pRecBuf_index_insert_inmemory(isax_index *index,
                                    sax_type * sax,
                                    file_position_type * pos,pthread_mutex_t *lock_firstnode,int workernumber,int total_workernumber);
root_mask_type isax_pRecBuf_index_insert_inmemory_ekosmas(isax_index *index,
                                    sax_type * sax,
                                    file_position_type * pos,pthread_mutex_t *lock_firstnode,int workernumber,int total_workernumber);
root_mask_type isax_single_pRecBuf_index_insert_inmemory_geopat(isax_index *index,
                                    sax_type * sax,
                                    file_position_type * pos,pthread_mutex_t *lock_firstnode,int workernumber,int total_workernumber,unsigned long number_of_series,unsigned long time_series_num,int flag);									

enum response flush_subtree_leaf_buffers_inmemory (isax_index *index, isax_node *node);

void help_subtree_path_calculation(isax_node_single_buffer * node );
void calculate_subtree_nodes(isax_node_single_buffer * node);
void traverseRootChilds(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node);
void printSubtree(isax_node *node,isax_index *index);
void checkSubtree(isax_node_single_buffer *node);
void checkTree(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node);
void fillCheckRawBufferFromTree(isax_node *node, int *array);

unsigned long populate_tree_lock_free_announce(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, unsigned long my_id, const char is_helper, const char lockfree_parallelism_in_subtree);
unsigned long populate_tree_with_locks(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, unsigned long my_id, const char is_helper);
unsigned long populate_tree_lock_free_cow(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, unsigned long my_id, const char is_helper);

void check_validity_ekosmas_lf(isax_index *index, long int ts_num, const char parallelism_in_subtree);

typedef struct					// MESSI receive buffer for process worker number
{
	isax_index *index;
	int start_number,stop_number;
	ts_type * ts;
	pthread_mutex_t *lock_record;
	pthread_mutex_t *lock_fbl;
	pthread_mutex_t *lock_index;
	pthread_mutex_t *lock_cbl;
	pthread_mutex_t *lock_firstnode;							// 
	pthread_mutex_t *lock_nodeconter;
	pthread_mutex_t *lock_disk;
	int workernumber;											// worker processing this buffer
	int total_workernumber;										// 
	pthread_barrier_t *lock_barrier1, *lock_barrier2;				
	int *node_counter;
	// bool finished;
	int *nodeid;
	unsigned long *shared_start_number;
    int myid;
} buffer_data_inmemory;



typedef struct
{
	isax_index *index;
	unsigned long  ts_num;
	int workernumber;
	pthread_mutex_t *lock_firstnode;
	pthread_barrier_t *wait_summaries_to_compute;				
	int *node_counter;
	int *finish_raw_buffer;
	// bool finished;
	unsigned long *shared_start_number;
	char parallelism_in_subtree;
	volatile unsigned long *next_iSAX_group;
	int subtree_parallelism; //geopat
	int chunk_size; //Geopat for chunk size algorithms
	int helping; // Boolean for enabling or disabling helping for receive buffer
	unsigned long  num_of_helping_series_buff;
	long *start_count;
	long *end_count;
	//needed for phase 2
	int *finish_rec_buff_phase2;
	unsigned long *shared_start_number_phase2; // Fetch and add blocks
	int backoff;
	
} buffer_data_inmemory_ekosmas;


void check_help_processing_chunk_tree(unsigned long my_ts_start,unsigned long my_ts_end,parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,
                                        isax_index *index,buffer_data_inmemory_ekosmas *input_data);

typedef struct
{
	isax_index *index;
	int ts_num;
	int workernumber;
	pthread_barrier_t *wait_summaries_to_compute;				
	int *node_counter;
	int *test_counter;
	// bool finished;
	unsigned long *shared_start_number;
	char parallelism_in_subtree;
	//Tree Construction Flag Geopat + Chunks size.
	int helping;
	unsigned long *shared_start_number_phase2; // Fetch and add blocks	
} buffer_data_inmemory_ekosmas_lf;

typedef struct transferfblinmemory
{
	int start_number,stop_number,conternumber;
  	int preworkernumber;
  	isax_index *index;
  	int *nodeid;
}transferfblinmemory;

float *rawfile;
    //////////////////////////////////////////////
long int * attrfile;
    //////////////////////////////////////////////

// ----------------------------------------------
// ekosmas:
// ----------------------------------------------
volatile unsigned char *block_processed;
volatile unsigned char *block_processed_sum_buffer;
volatile unsigned char *ts_processed;

typedef struct next_ts_grp {
	volatile unsigned long num CACHE_ALIGN;
	char pad[PAD_CACHE(sizeof(unsigned long))];
} next_ts_group;

extern next_ts_group *next_ts_group_read_in_block;
extern next_ts_group *next_ts_group_read_in_block_sum_buffer;

extern volatile unsigned char all_blocks_processed;
extern volatile unsigned char all_RecBufs_processed;
extern volatile unsigned char *block_helper_exist;
extern volatile unsigned char *block_helper_exist_sum_buffer;
extern volatile unsigned char *block_helpers_num;
extern volatile unsigned char *recBuf_helpers_num;

static __thread double my_time_for_blocks_processed = 0;
static __thread unsigned long my_num_blocks_processed = 0;
static __thread double my_time_for_subtree_construction = 0;
static __thread unsigned long my_num_subtree_construction = 0;
static __thread unsigned long my_num_subtree_nodes = 0;
static __thread struct timeval my_time_start_val;
static __thread struct timeval my_current_time_val;
static __thread double my_tS;
static __thread double my_tE;

#define COUNT_MY_TIME_START            gettimeofday(&my_time_start_val, NULL);
#define COUNT_MY_TIME_FOR_BLOCKS_END   gettimeofday(&my_current_time_val, NULL); \
                                            my_tS = my_time_start_val.tv_sec*1000000 + (my_time_start_val.tv_usec); \
                                            my_tE = my_current_time_val.tv_sec*1000000 + (my_current_time_val.tv_usec); \
                                            my_time_for_blocks_processed += (my_tE - my_tS);
#define COUNT_MY_TIME_FOR_SUBTREE_END  gettimeofday(&my_current_time_val, NULL); \
                                            my_tS = my_time_start_val.tv_sec*1000000 + (my_time_start_val.tv_usec); \
                                            my_tE = my_current_time_val.tv_sec*1000000 + (my_current_time_val.tv_usec); \
                                            my_time_for_subtree_construction += (my_tE - my_tS);    
#define BACKOFF_BLOCK_DELAY_VALUE	   (my_time_for_blocks_processed/my_num_blocks_processed)
#define BACKOFF_SUBTREE_DELAY_PER_NODE (my_time_for_subtree_construction/my_num_subtree_nodes)

static __thread unsigned long blocks_helped_cnt = 0;
static __thread unsigned long blocks_helping_avoided_cnt = 0;
static __thread unsigned long recBufs_helped_cnt = 0;
static __thread unsigned long recBufs_helping_avoided_cnt = 0;


// ----------------------------------------------


typedef struct node_list
{
	void **nlist; //isax_node
	int node_amount;
} node_list;

typedef struct node_list_lf
{
	parallel_fbl_soft_buffer_ekosmas_lf **nlist;
	int node_amount;
} node_list_lf;

#endif
